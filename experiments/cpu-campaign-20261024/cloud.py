#!/usr/bin/env python3
"""Campaign-owned EC2 leases; never modifies another experiment's resources.

Private state/keys belong in ignored data/, not beside this reviewable controller.
Every instance shuts itself down independently of this client, and EC2 converts
that shutdown to termination. Check the guest timer before starting any work.
"""
import argparse
import base64
import datetime as dt
import fcntl
import ipaddress
import json
import math
import pathlib
import shlex
import subprocess
import time
import urllib.request

REPO = pathlib.Path(__file__).resolve().parents[2]
ROOT = REPO / "data/cpu-campaign-20261024"
EXPERIMENT = "cpu-campaign-20261024"
DEADLINE = dt.datetime(2026, 10, 24, 20, 0, tzinfo=dt.timezone.utc)
TYPES = {"m6g.4xlarge": "arm64", "m7g.4xlarge": "arm64", "m8g.4xlarge": "arm64",
         "c4.4xlarge": "amd64", "c5.4xlarge": "amd64",
         "m5a.4xlarge": "amd64", "c5a.4xlarge": "amd64",
         "c6i.4xlarge": "amd64", "c7i.4xlarge": "amd64", "c8i.4xlarge": "amd64"}


def now():
    return dt.datetime.now(dt.timezone.utc)


def save(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2) + "\n")
    tmp.replace(path)


def aws(service, action, *args):
    result = subprocess.run(["aws", "--profile", "seine-bench", "--region", "us-east-1",
                             service, action, *args, "--output", "json"],
                            capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout or "{}")


def state():
    path = ROOT / "cloud-leases.json"
    return json.loads(path.read_text()) if path.exists() else {"leases": [], "security_group": None}


def persist(s):
    save(ROOT / "cloud-leases.json", s)


def finalize_reserve(record):
    """Release unused contingency only after instance AND disk deletion proof.

    Closed compute/storage/IP time is rounded UP to whole hours, with the full
    $1 transfer allowance retained. This is a conservative commitment, not an
    invoice. Original three-hour reservations remain auditable in the record.
    """
    assert record.get('terminated') and record.get('volumes_deleted')
    assert record.get('volume_ids'), 'unknown disk identities cannot release a reservation'
    if record.get('reservation_finalized_at'):
        return 0.0
    elapsed = (dt.datetime.fromisoformat(record['verified_at'])
               - dt.datetime.fromisoformat(record['launched_at'])).total_seconds()
    assert 0 <= elapsed <= 3 * 3600, 'do not reduce an unbounded/late-cleanup reservation'
    hours = max(1, math.ceil(elapsed / 3600))
    closed = (record['hourly_usd'] + .005 + 16 * .08 / (28 * 24)) * hours + 1
    initial = record['cost_reserved_usd']
    assert closed <= initial + 1e-9
    record.update(initial_cost_reserved_usd=initial, cost_reserved_usd=closed,
                  released_reserve_usd=initial - closed, closed_billing_hours_upper=hours,
                  reservation_finalized_at=now().isoformat())
    return initial - closed


def finalize_budget():
    s = state()
    before = sum(r['cost_reserved_usd'] for r in s['leases'])
    released = sum(finalize_reserve(r) for r in s['leases'] if r.get('terminated'))
    after = sum(r['cost_reserved_usd'] for r in s['leases'])
    s.setdefault('budget_history', []).append({'at': now().isoformat(),
        'action': 'finalize verified terminated reservations', 'before_usd': before,
        'after_usd': after, 'released_usd': released})
    persist(s)
    print(json.dumps({'before_usd': before, 'after_usd': after, 'released_usd': released}))


def quote(instance_type):
    filters = [{"Type": "TERM_MATCH", "Field": key, "Value": value} for key, value in {
        "instanceType": instance_type, "location": "US East (N. Virginia)",
        "operatingSystem": "Linux", "tenancy": "Shared", "preInstalledSw": "NA",
        "capacitystatus": "Used"}.items()]
    raw = aws("pricing", "get-products", "--service-code", "AmazonEC2", "--filters", json.dumps(filters))
    retrieved = now()
    save(ROOT / f"price-{instance_type}-{retrieved.strftime('%H%M%S')}.json", raw)
    values = []
    for entry in raw["PriceList"]:
        product = json.loads(entry) if isinstance(entry, str) else entry
        for term in product.get("terms", {}).get("OnDemand", {}).values():
            for dimension in term["priceDimensions"].values():
                if dimension["unit"] == "Hrs":
                    values.append(float(dimension["pricePerUnit"]["USD"]))
    assert len(values) == 1 and values[0] > 0, values
    record = {"usd_per_hour": values[0], "region": "us-east-1", "source": "AWS Pricing API",
              "retrieved_utc": retrieved.isoformat()}
    prices = json.loads((ROOT / "prices.json").read_text())
    prices[instance_type] = record
    save(ROOT / "prices.json", prices)
    return record


def owned(ident):
    record = next(x for x in state()["leases"] if x["instance_id"] == ident)
    info = aws("ec2", "describe-instances", "--instance-ids", ident)["Reservations"][0]["Instances"][0]
    assert {t["Key"]: t["Value"] for t in info.get("Tags", [])}.get("Experiment") == EXPERIMENT
    return record, info


def ssh(ident, command, timeout=60):
    _, info = owned(ident)
    arguments = ["ssh", "-i", str(ROOT / "ssh-key"), "-o", "BatchMode=yes",
                             "-o", "StrictHostKeyChecking=accept-new", "-o",
                             f"UserKnownHostsFile={ROOT}/known-hosts", "-o", "ConnectTimeout=10",
                             "ubuntu@" + info["PublicIpAddress"], command]
    deadline = time.monotonic() + timeout
    for attempt in range(3):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("SSH connection retry deadline exceeded")
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=remaining)
        if not result.returncode:
            return result.stdout
        # Only this exact pre-connection failure proves the remote command did
        # not start. Never replay a command after execution/connection loss.
        preconnect_timeout = (result.returncode == 255 and not result.stdout
            and result.stderr.strip() == f"ssh: connect to host {info['PublicIpAddress']} port 22: Connection timed out")
        if preconnect_timeout and attempt < 2:
            time.sleep(min(5, max(0, deadline - time.monotonic())))
            continue
        raise RuntimeError(f"SSH exit {result.returncode}: {result.stderr[-2000:]} {result.stdout[-2000:]}")


def copy(ident, source, destination, download=False):
    _, info = owned(ident)
    host = "ubuntu@" + info["PublicIpAddress"] + ":"
    subprocess.run(["scp", "-q", "-r", "-i", str(ROOT / "ssh-key"), "-o", "BatchMode=yes",
                    "-o", "StrictHostKeyChecking=accept-new", "-o",
                    f"UserKnownHostsFile={ROOT}/known-hosts", "-o", "ConnectTimeout=10",
                    host + source if download else source,
                    destination if download else host + destination], check=True, timeout=300)


def launch(instance_type, minutes):
    assert instance_type in TYPES and 15 <= minutes <= 150
    end = min(now() + dt.timedelta(minutes=minutes), DEADLINE)
    assert end - now() > dt.timedelta(minutes=15), "campaign deadline too close"
    minutes = int((end - now()).total_seconds() // 60)
    # The caller must coordinate with other sessions before this check. A quota
    # failure does not trigger retries that could race another experiment.
    limit = aws("service-quotas", "get-service-quota", "--service-code", "ec2",
                "--quota-code", "L-1216C47A")["Quota"]["Value"]
    running = aws("ec2", "describe-instances", "--filters",
                  "Name=instance-state-name,Values=pending,running")["Reservations"]
    standard = [i for r in running for i in r["Instances"]
                if i["InstanceType"][0] in "acdhimrtz"]
    names = sorted({i["InstanceType"] for i in standard} | {instance_type})
    specs = aws("ec2", "describe-instance-types", "--instance-types", *names)["InstanceTypes"]
    cpus = {i["InstanceType"]: i["VCpuInfo"]["DefaultVCpus"] for i in specs}
    assert sum(cpus[i["InstanceType"]] for i in standard) + cpus[instance_type] <= limit, "shared quota occupied"
    price = quote(instance_type)  # Refresh before every rental, not just campaign start.
    # Price API artifacts are retained separately. Reserve a full three hours,
    # 16 GiB gp3 ($.08/GB-month), IPv4 ($.005/h), and $1 transfer contingency.
    hourly = float(price["usd_per_hour"])
    reserve = (hourly + .005 + 16 * .08 / (28 * 24)) * 3 + 1
    s = state()
    assert sum(x["cost_reserved_usd"] for x in s["leases"]) + reserve <= 40, "campaign budget exhausted"
    if not (ROOT / "ssh-key").exists():
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                        str(ROOT / "ssh-key"), "-C", EXPERIMENT], check=True)
    pubkey = (ROOT / "ssh-key.pub").read_text().strip()
    vpc = aws("ec2", "describe-vpcs", "--filters", "Name=is-default,Values=true")["Vpcs"][0]["VpcId"]
    if not s["security_group"]:
        address = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=15).read().decode().strip()
        ipaddress.IPv4Address(address)
        # This is the existing benchmark role's required resource-tag namespace.
        tags = [{"ResourceType": "security-group", "Tags": [
            {"Key": "Project", "Value": "seine-avx2-validation"},
            {"Key": "Experiment", "Value": EXPERIMENT}]}]
        group = aws("ec2", "create-security-group", "--group-name", "seine-" + EXPERIMENT,
                    "--description", "Temporary Seine CPU campaign SSH", "--vpc-id", vpc,
                    "--tag-specifications", json.dumps(tags))["GroupId"]
        s["security_group"] = group
        persist(s)  # Record before ingress, so failure cannot lose cleanup ownership.
        aws("ec2", "authorize-security-group-ingress", "--group-id", group,
            "--protocol", "tcp", "--port", "22", "--cidr", address + "/32")
    offers = aws("ec2", "describe-instance-type-offerings", "--location-type", "availability-zone",
                 "--filters", "Name=instance-type,Values=" + instance_type)["InstanceTypeOfferings"]
    zones = {x["Location"] for x in offers}
    subnets = aws("ec2", "describe-subnets", "--filters", "Name=vpc-id,Values=" + vpc,
                  "Name=default-for-az,Values=true")["Subnets"]
    subnets = sorted((x for x in subnets if x["AvailabilityZone"] in zones), key=lambda x: x["AvailabilityZone"])
    # Schedule termination FIRST; build/toolchain failures cannot bypass it.
    userdata = f"""#!/bin/bash
set -eu
remaining=$(( ({int(end.timestamp())} - $(date +%s)) / 60 ))
if [ "$remaining" -le 0 ]; then shutdown -h now; exit 0; fi
shutdown -h +"$remaining"
install -d -m 700 -o ubuntu -g ubuntu /home/ubuntu/.ssh
printf '%s\\n' {shlex.quote(pubkey)} >> /home/ubuntu/.ssh/authorized_keys
chown ubuntu:ubuntu /home/ubuntu/.ssh/authorized_keys
chmod 600 /home/ubuntu/.ssh/authorized_keys
"""
    tags = [{"Key": "Experiment", "Value": EXPERIMENT}, {"Key": "Project", "Value": "seine-avx2-validation"},
            {"Key": "Name", "Value": "seine-campaign-" + instance_type},
            {"Key": "ExpiresAt", "Value": end.isoformat()}]
    ami = (ROOT / (TYPES[instance_type] + "-ami.txt")).read_text().strip()
    token = EXPERIMENT + "-" + instance_type + "-" + str(int(time.time()))
    for subnet in subnets:
        spec = {"ImageId": ami, "InstanceType": instance_type, "MinCount": 1, "MaxCount": 1,
                "ClientToken": token + "-" + subnet["AvailabilityZone"],
                "SubnetId": subnet["SubnetId"], "SecurityGroupIds": [s["security_group"]],
                "InstanceInitiatedShutdownBehavior": "terminate", "MetadataOptions": {"HttpTokens": "required"},
                "UserData": base64.b64encode(userdata.encode()).decode(),
                "BlockDeviceMappings": [{"DeviceName": "/dev/sda1", "Ebs": {
                    "VolumeSize": 16, "VolumeType": "gp3", "DeleteOnTermination": True}}],
                "TagSpecifications": [{"ResourceType": t, "Tags": tags} for t in ["instance", "volume"]]}
        try:
            answer = aws("ec2", "run-instances", "--cli-input-json", json.dumps(spec))
            break
        except RuntimeError as error:
            if "InsufficientInstanceCapacity" not in str(error) and "Unsupported" not in str(error):
                raise
    else:
        raise RuntimeError("no capacity in offered zones")
    instance = answer["Instances"][0]
    record = {"instance_id": instance["InstanceId"], "type": instance_type,
              "hourly_usd": hourly, "cost_reserved_usd": reserve, "launched_at": instance["LaunchTime"],
              "price_retrieved_utc": price["retrieved_utc"],
              "expires_at": end.isoformat(), "volume_ids": [], "timer_verified": False}
    s["leases"].append(record)
    s.setdefault('budget_history', []).append({'at': now().isoformat(),
        'action': 'launch reservation',
        'after_usd': sum(r['cost_reserved_usd'] for r in s['leases'])})
    persist(s)
    save(ROOT / (record["instance_id"] + "-launch.json"), answer)
    print(json.dumps(record), flush=True)
    return record


def ready(ident):
    deadline = time.monotonic() + 360
    while time.monotonic() < deadline:
        try:
            text = ssh(ident, "cloud-init status --wait; sudo cat /run/systemd/shutdown/scheduled", timeout=30)
            assert "USEC=" in text and "MODE=poweroff" in text, "independent shutdown timer missing"
            s = state()
            record = next(x for x in s["leases"] if x["instance_id"] == ident)
            _, info = owned(ident)
            behavior = aws("ec2", "describe-instance-attribute", "--instance-id", ident,
                           "--attribute", "instanceInitiatedShutdownBehavior")["InstanceInitiatedShutdownBehavior"]["Value"]
            assert behavior == "terminate", "guest shutdown would not terminate the rental"
            scheduled = next(int(line.split("=", 1)[1]) / 1_000_000
                             for line in text.splitlines() if line.startswith("USEC="))
            assert now().timestamp() < scheduled <= dt.datetime.fromisoformat(record["expires_at"]).timestamp() + 5
            record.update(timer_verified=True, timer_evidence=text, ip=info["PublicIpAddress"],
                          shutdown_behavior=behavior,
                          volume_ids=[x["Ebs"]["VolumeId"] for x in info["BlockDeviceMappings"]])
            persist(s)
            print(text)
            return
        except (RuntimeError, KeyError, subprocess.TimeoutExpired):
            time.sleep(5)
    raise RuntimeError("startup failed; terminate this lease")


def terminate(ident):
    record, info = owned(ident)
    if not record["volume_ids"]:
        record["volume_ids"] = [x["Ebs"]["VolumeId"] for x in info.get("BlockDeviceMappings", [])]
    aws("ec2", "terminate-instances", "--instance-ids", ident)
    for _ in range(60):
        _, info = owned(ident)
        if info["State"]["Name"] == "terminated":
            break
        time.sleep(5)
    assert info["State"]["Name"] == "terminated"
    assert record['volume_ids'], 'instance terminated, but disk identities are unknown; retain its reservation'
    for _ in range(12):
        volumes = aws("ec2", "describe-volumes", "--filters",
                      "Name=volume-id,Values=" + ','.join(record['volume_ids']))["Volumes"]
        if not volumes:
            break
        time.sleep(5)
    assert not volumes, 'known campaign disk still exists'
    s = state()
    saved = next(x for x in s["leases"] if x["instance_id"] == ident)
    verified = now().isoformat()
    saved.update(volume_ids=record["volume_ids"], terminated=True, volumes_deleted=True)
    if saved.get('verified_at'):
        saved['cleanup_rechecked_at'] = verified
    else:
        saved['verified_at'] = verified
    # Keep obtained cleanup proof even if conservative reconciliation refuses
    # to release budget (for example, a verification delayed beyond three hours).
    persist(s)
    released = finalize_reserve(saved)
    s.setdefault('budget_history', []).append({'at': now().isoformat(),
        'action': 'finalize verified terminated reservation', 'released_usd': released,
        'after_usd': sum(r['cost_reserved_usd'] for r in s['leases'])})
    persist(s)
    print(json.dumps(saved))


def cleanup():
    s = state()
    remaining = aws("ec2", "describe-instances", "--filters", "Name=tag:Experiment,Values=" + EXPERIMENT,
                    "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down")["Reservations"]
    assert not any(r["Instances"] for r in remaining), "live campaign instances remain"
    assert not aws("ec2", "describe-volumes", "--filters", "Name=tag:Experiment,Values=" + EXPERIMENT)["Volumes"]
    if s["security_group"]:
        aws("ec2", "delete-security-group", "--group-id", s["security_group"])
        s["security_group_deleted"] = s["security_group"]
        s["security_group"] = None
        persist(s)
    groups = aws("ec2", "describe-security-groups", "--filters",
                 "Name=tag:Experiment,Values=" + EXPERIMENT)["SecurityGroups"]
    assert not groups, "campaign security group still exists"
    for filename in ["ssh-key", "ssh-key.pub", "known-hosts", "known-hosts.old"]:
        (ROOT / filename).unlink(missing_ok=True)
        assert not (ROOT / filename).exists()
    s["security_group_absence_verified"] = True
    s["local_ssh_material_removed"] = True
    s["cleanup_verified_at"] = now().isoformat()
    persist(s)
    print(json.dumps(s))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["launch", "ready", "ssh", "upload", "download", "terminate", "cleanup", "status", "finalize-budget"])
    p.add_argument("args", nargs="*")
    a = p.parse_args()
    # State mutations serialize, preventing two local launches from overspending.
    with (ROOT / "cloud.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if a.action == "launch": launch(a.args[0], int(a.args[1]) if len(a.args) > 1 else 150)
        elif a.action == "ready": ready(a.args[0])
        elif a.action == "ssh": print(ssh(a.args[0], a.args[1], timeout=300))
        elif a.action in ("upload", "download"): copy(*a.args, download=a.action == "download")
        elif a.action == "terminate": terminate(a.args[0])
        elif a.action == "cleanup": cleanup()
        elif a.action == "finalize-budget": finalize_budget()
        else: print(json.dumps(state(), indent=2))


if __name__ == "__main__":
    main()
