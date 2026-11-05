export const SITE_URL = 'https://seineminer.com';
export const REPO = 'https://github.com/zpalmtree/seine';
// Bump the query when the card changes: link previews cache images by URL.
export const DEFAULT_IMAGE = `${SITE_URL}/assets/brand/social-card.jpg?v=2`;

export const ORGANIZATION = {
    '@type': 'Organization',
    '@id': `${SITE_URL}/#org`,
    name: 'seine',
    url: `${SITE_URL}/`,
    logo: { '@type': 'ImageObject', url: `${SITE_URL}/assets/brand/icon-512.png` },
    sameAs: [REPO],
};

/** Serialize JSON-LD for a <script> tag without letting "</" close it early. */
export const jsonLd = (data: unknown) => JSON.stringify(data).replace(/</g, '\\u003c');

export const isoDate = (d: Date) => d.toISOString().slice(0, 10);

export const formatDate = (d: Date, htmlLang: string) =>
    new Intl.DateTimeFormat(htmlLang, { calendar: 'gregory', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC' }).format(d);

export function breadcrumbList(items: [string, string][]) {
    return {
        '@context': 'https://schema.org',
        '@type': 'BreadcrumbList',
        itemListElement: items.map(([name, item], i) => ({ '@type': 'ListItem', position: i + 1, name, item })),
    };
}
