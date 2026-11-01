#!/usr/bin/env python3
"""Exercise an extracted Seine binary against loopback-only, job-free fixtures."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import threading

if os.name != "nt":
    import pty
import signal
import socket
import subprocess
import sys
import tempfile
import time

# A known public shared address; every memo below is synthetic.
EXCHANGE = 'ZJZqGfDPCVg78CRH5Z33CtHafg911Dp5Bauf1Dnxg5jgXx7Whqsvqka96FCV8W6QZXNDKzJefkFU6bwDEGBewqPrTWjij'
ORDINARY = 'SEiNEceuDyfEY3GKDQAYVuK6382K2Ln2gQSv83ySAFkKhraBxhHnTf2P6PsF8CXtreqywg4T1qwBAjQo3L2VYDYWtsbb9'
MEMO = 'DEMO001'


def listener():
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen()
    server.settimeout(20)
    return server


def url(server):
    return 'stratum+tcp://127.0.0.1:' + str(server.getsockname()[1])


class Miner:
    def __init__(self, binary, directory, address, pools, memo=None, interactive=False):
        self.config = directory / 'seine.config.json'
        saved = dict(schema_version=1, mode='pool', address=address, pool_url=pools,
                     pool_worker='rollout-fixture', dev_fee_pool_worker='rollout-fixture-dev')
        if memo is not None:
            saved['payout_memo'] = memo
        self.config.write_text(json.dumps(saved))
        self.original = self.config.read_bytes()
        self.output = b''
        args = [str(binary), '--data-dir', str(directory), '--backend', 'cpu', '--threads', '1',
                '--ui', 'plain', '--disable-cpu-autotune-threads', '--disable-update-check', '--telemetry', 'off']
        env = dict(os.environ, NO_COLOR='1', CLICOLOR='0', CLICOLOR_FORCE='0', TERM='dumb')
        if interactive:
            self.fd, slave = pty.openpty()
            try:
                self.process = subprocess.Popen(args, stdin=slave, stdout=slave, stderr=slave, start_new_session=True, env=env)
            finally:
                os.close(slave)
        else:
            self.process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT, start_new_session=True, env=env)
            self.fd = self.process.stdout.fileno()
        self.interactive = interactive
        self.chunks = queue.Queue()
        def read_output():
            try:
                while True:
                    chunk = os.read(self.fd, 65536)
                    self.chunks.put(chunk)
                    if not chunk:
                        return
            except OSError:
                # POSIX terminal masters report EIO when the child closes the slave.
                self.chunks.put(b'')
        self.reader = threading.Thread(target=read_output, daemon=True)
        self.reader.start()

    def drain_output(self):
        while True:
            try:
                self.output += self.chunks.get_nowait()
            except queue.Empty:
                return

    def wait_text(self, text, timeout=20):
        deadline = time.monotonic() + timeout
        while text.encode() not in self.output and time.monotonic() < deadline:
            try:
                chunk = self.chunks.get(timeout=0.1)
            except queue.Empty:
                continue
            if not chunk:
                break
            self.output += chunk
        if text.encode() not in self.output:
            raise AssertionError(f'Missing {text!r}: {self.output.decode(errors="replace")[-2500:]}')

    def report_failure(self, name):
        self.drain_output()
        print(f'{name} failed (miner exit={self.process.poll()}):\n'
              f'{self.output.decode(errors="replace")[-5000:]}', file=sys.stderr, flush=True)

    def close(self):
        if self.process.poll() is None:
            if os.name == 'nt':
                self.process.terminate()
            else:
                os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name == 'nt':
                    self.process.kill()
                else:
                    os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        if self.reader is not None:
            self.reader.join(timeout=5)
        if self.interactive:
            os.close(self.fd)
        else:
            self.process.stdout.close()


def login(server, address, memo, minimum=1_000_000_000):
    conn, _ = server.accept()
    conn.settimeout(10)
    try:
        with conn.makefile('rb') as stream:
            request = json.loads(stream.readline())
        params = request['params']
        assert request['method'] == 'login'
        assert params['address'] == address
        assert params.get('payout_memo') == memo
        result = dict(protocol_version=2, capabilities=['submit_claimed_hash'], required_capabilities=[])
        if memo:
            encoded = address.encode()
            account = hashlib.sha3_256(b'blocknet_pool_payout_memo_v1\0' + len(encoded).to_bytes(8, 'big') + encoded + memo.encode()).hexdigest()
            result.update(payout_account='memo:' + account, capabilities=['submit_claimed_hash', 'payout_memo_v1'])
        if minimum is not None:
            result['min_payout_atomic'] = minimum
        conn.sendall((json.dumps(dict(id=request['id'], status='ok', result=result))+'\n').encode())
        return conn
    except BaseException:
        conn.close()
        raise


def check(binary, skip_interactive=False):
    checks = []
    for name, address, memo, interactive, minimum in [
        ('ordinary_saved_config', ORDINARY, None, False, 100_000_000),
        ('saved_exchange_prompt', EXCHANGE, None, True, 1_000_000_000),
        ('memo_saved_config', EXCHANGE, MEMO, False, 1_000_000_000),
        ('memo_unknown_minimum', EXCHANGE, MEMO, False, None),
        ('unattended_missing_memo', EXCHANGE, None, False, None),
    ]:
        if interactive and skip_interactive:
            continue
        with tempfile.TemporaryDirectory(prefix='seine-memo-check-') as tmp, listener() as server:
            print(f'Checking {name}', file=sys.stderr, flush=True)
            miner = Miner(binary, Path(tmp), address, url(server), memo, interactive)
            try:
                if name == 'unattended_missing_memo':
                    miner.wait_text('requires a memo')
                    assert miner.process.wait(timeout=5) != 0
                    assert miner.config.read_bytes() == miner.original
                    server.settimeout(0.2)
                    try:
                        conn, _ = server.accept()
                    except socket.timeout:
                        pass
                    else:
                        conn.close()
                        raise AssertionError('Missing-memo process connected to pool')
                else:
                    if interactive:
                        miner.wait_text('Enter your deposit memo:')
                        os.write(miner.fd, (MEMO+'\n').encode())
                        memo = MEMO
                    with login(server, address, memo, minimum):
                        miner.wait_text('login accepted')
                        if minimum is not None:
                            miner.wait_text(f'Minimum payout: {minimum // 100_000_000} BNT.')
                        else:
                            assert b'Minimum payout:' not in miner.output
                    if interactive:
                        assert json.loads(miner.config.read_text())['payout_memo'] == MEMO
                    else:
                        assert miner.config.read_bytes() == miner.original
                checks.append(name)
            except BaseException:
                miner.report_failure(name)
                raise
            finally:
                miner.close()
    # Failed primary, accepted backup, then reconnect: the memo must survive each login.
    with tempfile.TemporaryDirectory(prefix='seine-memo-check-') as tmp, listener() as primary, listener() as backup:
        print('Checking memo_failover_and_reconnect', file=sys.stderr, flush=True)
        miner = Miner(binary, Path(tmp), EXCHANGE, url(primary)+','+url(backup), MEMO)
        try:
            for _ in range(2):
                conn, _ = primary.accept()
                with conn:
                    conn.settimeout(10)
                    with conn.makefile('rb') as stream:
                        request = json.loads(stream.readline())
                    assert request['params']['payout_memo'] == MEMO
                    conn.sendall((json.dumps(dict(id=request['id'], error='fixture rejection'))+'\n').encode())
            with login(backup, EXCHANGE, MEMO):
                miner.wait_text('login accepted', timeout=30)
            miner.output = b''
            with login(backup, EXCHANGE, MEMO, None):
                miner.wait_text('login accepted', timeout=30)
                assert b'Minimum payout:' not in miner.output
            checks.append('memo_failover_and_reconnect')
        except BaseException:
            miner.report_failure('memo_failover_and_reconnect')
            raise
        finally:
            miner.close()
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--skip-interactive', action='store_true', help='Skip the POSIX terminal prompt fixture')
    args = parser.parse_args()
    if os.name == 'nt' and not args.skip_interactive:
        parser.error('Windows requires --skip-interactive; prompt fixture uses a POSIX terminal')
    binary = args.binary.resolve(strict=True)
    print(json.dumps({'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                      'passed': check(binary, args.skip_interactive),
                      'skipped': ['saved_exchange_prompt'] if args.skip_interactive else [], 'jobs_sent': 0}, indent=2))


if __name__ == '__main__':
    main()
