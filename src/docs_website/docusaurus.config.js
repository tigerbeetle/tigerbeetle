// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'TigerBeetle Docs',
  tagline: '',
  url: 'https://docs.tigerbeetle.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.png',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'tigerbeetle', // Usually your GitHub org/user name.
  projectName: 'docs', // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          path: 'pages',
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl: ({ docPath }) =>
            'https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/' + docPath.replace('/pages/', ''),
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        blog: false,
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    {
      algolia: {
        appId: 'NPDIZGXHAP',
        apiKey: 'c31d9000a1856050585f6b9a1a1a4eb8',
        indexName: 'tigerbeetle',
        contextualSearch: true,
        searchPagePath: 'search',
      },
      navbar: {
        title: 'TigerBeetle Docs',
        logo: {
          alt: 'TigerBeetle Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            href: 'https://github.com/tigerbeetle/tigerbeetle',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Community',
            items: [
              {
                label: 'Twitter',
                href: 'https://twitter.com/tigerbeetledb',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/tigerbeetle/tigerbeetle',
              },
              {
                label: 'LinkedIn',
                href: 'https://linkedin.com/company/tigerbeetle',
              },
              {
                label: 'Discord',
                href: 'https://discord.gg/uWCGp46uG5',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} TigerBeetle, Inc. All rights reserved.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
        additionalLanguages: ['java', 'csharp', 'zig'],
      },
    },
  scripts: [
      {src: 'https://plausible.io/js/script.js', defer: true, 'data-domain': 'docs.tigerbeetle.com'},
  ],

  markdown: {
    mermaid: true,
  },

  themes: ['@docusaurus/theme-mermaid'],
};

module.exports = config;
