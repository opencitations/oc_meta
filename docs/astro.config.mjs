// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import rehypeExternalLinks from 'rehype-external-links';

export default defineConfig({
	site: 'https://opencitations.github.io',
	base: '/oc_meta',
	markdown: {
		rehypePlugins: [
			[rehypeExternalLinks, { target: '_blank', rel: ['noopener', 'noreferrer'] }]
		],
	},
	integrations: [
		starlight({
			title: 'OpenCitations Meta',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/opencitations/oc_meta' },
			],
			sidebar: [
				{
					label: 'Guides',
					items: [
						{ label: 'Getting started', slug: 'guides/getting_started' },
					],
				},
				{
					label: 'Core',
					items: [
						{ label: 'Configuration', slug: 'guides/configuration' },
						{ label: 'Preprocessing', slug: 'guides/preprocessing' },
						{ label: 'Processing', slug: 'guides/processing' },
						{ label: 'Verification', slug: 'guides/verification' },
						{ label: 'Generate CSV', slug: 'guides/generate_csv' },
					],
				},
				{
					label: 'Patches',
					items: [
						{ label: 'Editing entities', slug: 'guides/meta_editor' },
						{ label: 'hasNext fixer', slug: 'patches/hasnext_fixer' },
					],
				},
				{
					label: 'External data',
					items: [
						{ label: 'ORCID-DOI index', slug: 'guides/orcid_index' },
					],
				},
				{
					label: 'Merge',
					items: [
						{ label: 'Overview', slug: 'merge/overview' },
						{ label: 'Find duplicates', slug: 'merge/duplicates' },
						{ label: 'Group entities', slug: 'merge/group_entities' },
						{ label: 'Merge entities', slug: 'merge/merge_entities' },
						{ label: 'Verify merge', slug: 'merge/verify_merge' },
						{ label: 'Compact CSV', slug: 'merge/compact_csv' },
						{ label: 'Merge history', slug: 'merge/merge_history' },
					],
				},
				{
					label: 'Count',
					items: [
						{ label: 'Meta entities', slug: 'count/meta_entities' },
						{ label: 'Triples', slug: 'count/triples' },
					],
				},
				{
					label: 'Info dir',
					items: [
						{ label: 'Overview', slug: 'infodir/overview' },
						{ label: 'Generate', slug: 'infodir/gen' },
						{ label: 'Check', slug: 'infodir/check' },
					],
				},
				{
					label: 'Migration',
					items: [
						{ label: 'Extract subset', slug: 'migration/extract_subset' },
						{ label: 'RDF from export', slug: 'migration/rdf_from_export' },
						{ label: 'Provenance to N-Quads', slug: 'migration/provenance_to_nquads' },
					],
				},
				{
					label: 'Benchmark',
					items: [
						{ label: 'Running benchmarks', slug: 'benchmark/running' },
						{ label: 'Generating test data', slug: 'benchmark/data_generation' },
						{ label: 'Configuration', slug: 'benchmark/configuration' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'CSV format', slug: 'reference/csv_format' },
						{ label: 'Testing', slug: 'reference/testing' },
					],
				},
				{ label: 'Contributing', slug: 'contributing' },
			],
		}),
	],
});
