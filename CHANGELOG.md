# [3.0.0](https://github.com/opencitations/oc_meta/compare/v2.1.3...v3.0.0) (2026-06-27)


* refactor!: replace generate_rdf_files with rdf_files_only and remove Virtuoso bulk loading ([4afd05f](https://github.com/opencitations/oc_meta/commit/4afd05ff0583c21b79c1dbbb2b21bac79f791a0d))
* refactor!: replace generate_rdf_files with rdf_files_only in editor and check_results ([8a22b8a](https://github.com/opencitations/oc_meta/commit/8a22b8ad1fad0d84404a95494bab76a02625bd25))
* refactor!: simplify preprocess_input to redis-only ([f7ba999](https://github.com/opencitations/oc_meta/commit/f7ba999b3ee57caa4ce311ab4eca256304f1de54))
* refactor!: switch check_results output from text report to structured JSON ([68bbab2](https://github.com/opencitations/oc_meta/commit/68bbab2f4e48ac1efa36c86603526a7444bb55d0))
* refactor!(migration): replace predicate discovery with file-based entity loading in extract_subset ([de81278](https://github.com/opencitations/oc_meta/commit/de812787cd90b5b2990f965bd2e6f3757254050b))


### Bug Fixes

* add file generation control to Meta Editor and update related tests to check databases after files ([853ac5d](https://github.com/opencitations/oc_meta/commit/853ac5dc55a138ad89bb97097ecd1536a124910d))
* add ORCID and institutional emails to preferred citation ([4d9e493](https://github.com/opencitations/oc_meta/commit/4d9e4939bb567e2700031678dcd225e77d440a0b))
* add timeout to SPARQLClient to handle database unavailability ([c59160b](https://github.com/opencitations/oc_meta/commit/c59160b3f3182e73ebe89d74ba24ba88dff422a3))
* **benchmark:** use get_storage_total for box plot storage times ([becd998](https://github.com/opencitations/oc_meta/commit/becd998829d5d7bb7b2572346791f21e48ec1f45))
* **check_results:** improve resilience to database unavailability ([1f3db50](https://github.com/opencitations/oc_meta/commit/1f3db50f009bf345c8d7e5fccc1b19e6364e4e27))
* **check_results:** normalize hyphens in identifiers before SPARQL lookup ([2684fe6](https://github.com/opencitations/oc_meta/commit/2684fe658fe85eb44705e78d6d6ed4f72c3ef841))
* **check_results:** normalize identifiers before OMID lookup ([858327d](https://github.com/opencitations/oc_meta/commit/858327dcadb5623c609258f0d4517f8f98992fb6))
* **check_results:** verify output CSVs instead of input CSVs ([4a30a3d](https://github.com/opencitations/oc_meta/commit/4a30a3d52b63355fd8c8f8d9b22893c668613870))
* **ci:** resolve GitHub Pages deployment conflict between docs and coverage ([e05d063](https://github.com/opencitations/oc_meta/commit/e05d0638ab8b59c375368b5c028a967c5127ccf6))
* **ci:** use workflow_run trigger for coverage artifact synchronization ([37c2f00](https://github.com/opencitations/oc_meta/commit/37c2f006384a26d82b61dc468abc52e3b381e6d3))
* clean up generated input CSV files in benchmark and pass correct Redis parameters to CacheManager to fix connection warning ([74bfc8d](https://github.com/opencitations/oc_meta/commit/74bfc8dbcbc71dd0da5f1bfb09cf28c11d2b61c2))
* **coverage:** correct source scope and include run folder ([dbd2c38](https://github.com/opencitations/oc_meta/commit/dbd2c386ee97af60b39831e41eaed736de02e381))
* **creator:** query editor roles on the correct BR entity ([4a05520](https://github.com/opencitations/oc_meta/commit/4a0552055e8f0c1e664245a65c47af13a248419c))
* **creator:** skip duplicate role agents to prevent cyclic hasNext chains ([7424288](https://github.com/opencitations/oc_meta/commit/74242887779e63d0546514d9ba38298fdb1d6be1))
* **creator:** skip silenced fields when entities exist in triplestore ([8fa4ebc](https://github.com/opencitations/oc_meta/commit/8fa4ebc8c86719b7b03bd1aee3a7397bd6520a5f))
* **creator:** use split instead of replace for stripping id schema prefixes ([21fab53](https://github.com/opencitations/oc_meta/commit/21fab53455bfee0a8f4c3814f829c99a620c258d))
* **csv_generator_lite:** prevent OOM by removing unbounded cache ([560bfb0](https://github.com/opencitations/oc_meta/commit/560bfb09116c8f769749a4a91d874c72fdc3e97e))
* **csv_generator_lite:** replace multiprocessing with sequential execution, add bounded LRU cache ([bb75e7c](https://github.com/opencitations/oc_meta/commit/bb75e7cd6976e2d3c74b56e82c4d7f52f4aa61f8))
* **csv_generator_lite:** restore parallel processing with configurable workers ([49b9432](https://github.com/opencitations/oc_meta/commit/49b94323523e3f945ae3f837133f0fc4800fbe69))
* **deps:** bump oc-ocdm to 11.0.16 ([f3f100f](https://github.com/opencitations/oc_meta/commit/f3f100f2bfd9378342789ad7499a2e09036c65df))
* **deps:** update oc-ocdm to 11.0.14 and add explicit counter flushes ([52d0f23](https://github.com/opencitations/oc_meta/commit/52d0f230a18597c37fe4d8234c6dcc20680078df))
* **docs:** correct sidebar slug for rdf_to_nquads page ([9a8f509](https://github.com/opencitations/oc_meta/commit/9a8f5094b3665562c231ddb1c31c36b8fb1d21ab))
* **file_manager:** prevent infinite loop when reading header-only CSV files ([a6f097f](https://github.com/opencitations/oc_meta/commit/a6f097f5e4d020fe8f265e4a97ef37f70b80511d))
* **finder:** handle long contributor role chains ([6689334](https://github.com/opencitations/oc_meta/commit/66893343be4fd3fa75982c1d5529437dedf7f490))
* **finder:** prevent VVI query explosion by using identifier-to-URI mapping ([845c286](https://github.com/opencitations/oc_meta/commit/845c2868a00b4aea0b2bc5d5c39defc33614eedb))
* **finder:** query provenance endpoint for merged entity lookup ([cb97af9](https://github.com/opencitations/oc_meta/commit/cb97af9a5de8e758b630001569026d25cf92886a))
* **finder:** remove automatic AR chain corrections from ResourceFinder ([4e8c165](https://github.com/opencitations/oc_meta/commit/4e8c165043e14859837338c5ecd714da8ae25bc2))
* **generate_csv:** always rebuild Redis OMID set at startup ([07bbc74](https://github.com/opencitations/oc_meta/commit/07bbc74be34c24de1a22c88db06062e3bb86d30a))
* **group_entities:** add file range grouping and config-based parameters ([11a78c4](https://github.com/opencitations/oc_meta/commit/11a78c4402ce55423e097f048d8ac7f568240b72))
* implement per-CSV triplestore upload with parallel data and provenance ([ccf70c4](https://github.com/opencitations/oc_meta/commit/ccf70c4b7ca55734dd3ed57312b41d54ee18994e))
* **master_of_regex:** prevent stray bracket from being captured as id ([b040203](https://github.com/opencitations/oc_meta/commit/b040203481ae0b9537ed8b1c1ae6570397136097))
* **meta_process:** implement true parallelism with ProcessPoolExecutor ([d28d8db](https://github.com/opencitations/oc_meta/commit/d28d8db367d265acd3600ad78b8407902715812d))
* migrate from ConjunctiveGraph to Dataset for RDFLib 7.4.0 ([be33981](https://github.com/opencitations/oc_meta/commit/be3398197ea2baf48be9eb7e3679d026a73639fe))
* **migration:** correct off-by-one error in rdf_to_nquads progress bar ([3ffee01](https://github.com/opencitations/oc_meta/commit/3ffee01df69764ad2c8a333934251d4fb1fb8058))
* **migration:** limit chunked n-quads memory use ([985fe7a](https://github.com/opencitations/oc_meta/commit/985fe7a7b77f75d393be4355049b411f5885c2a0))
* **patch:** auto-merge omid mismatches only when entities are confirmed to match ([a4d6561](https://github.com/opencitations/oc_meta/commit/a4d6561ab6d66255cab76690ca39d17691941c5e))
* **patch:** chain moved editor ARs after existing ones on the container ([89c9b84](https://github.com/opencitations/oc_meta/commit/89c9b84eaef80d397be979b517b837f972859717))
* **patch:** detect duplicate editors during AR migration and fix duplicate partOf ([b8c5b8a](https://github.com/opencitations/oc_meta/commit/b8c5b8a363bda5f7e9f663a6f59c8d2b8ae5dd42))
* **patches:** handle SIGINT gracefully in add_string_datatype ([47fa790](https://github.com/opencitations/oc_meta/commit/47fa790daae2e212f78387267fa6dec384c38725))
* **patch:** group editor AR fixes by container and delete duplicate ARs ([1601f71](https://github.com/opencitations/oc_meta/commit/1601f711bfed23691822e9e0443e06f3624c09a9))
* **patch:** handle content entities with multiple frbr:partOf containers ([6d8ae9e](https://github.com/opencitations/oc_meta/commit/6d8ae9e13adf8311942e0f4b946448023b80a8bc))
* **patch:** repair SICI DOIs truncated by the oc_ds_converter suffix bug ([29cacb6](https://github.com/opencitations/oc_meta/commit/29cacb6540c68c8f234154487540b21007256585))
* replace multiprocessing with sequential processing and parallel I/O ([143484c](https://github.com/opencitations/oc_meta/commit/143484c737b9adf226dcdbb1d3223fbe087d7ea0))
* replace RDFlib graph parsing with string matching in check_results ([78183d9](https://github.com/opencitations/oc_meta/commit/78183d9ed8caca4772db94ea73d5fc9c025f5f62))
* resolve HTTP connection leaks in SPARQLWrapper queries ([83823e6](https://github.com/opencitations/oc_meta/commit/83823e66eb074698f6e2a5e7eddf29f94deb149f))
* suppress verbose Storer INFO logs in benchmark tool ([d773222](https://github.com/opencitations/oc_meta/commit/d7732220dc29c1536ce608c3b27c1edd9391ccdb))
* **test:** add missing SAMPLE_JSONLD and INVALID_JSONLD constants ([6bf6103](https://github.com/opencitations/oc_meta/commit/6bf610302e63c16000b58dc0865a007ea7d7ba85))
* **test:** align SPARQL result expectations with standard format ([51ddefc](https://github.com/opencitations/oc_meta/commit/51ddefcfc4031c61ab7cc260cdca852d060ac473))
* **tests:** replace deprecated rdflib Identifier with Node ([fdfe226](https://github.com/opencitations/oc_meta/commit/fdfe226a798b421d596dcff9794d46a193e946d6))
* **timer:** exclude preexisting entities from new entity count ([3f0c02f](https://github.com/opencitations/oc_meta/commit/3f0c02f86bd00ab1567c08a83f489164a7b27765))
* Update check_results.py to read input directory from config ([985a251](https://github.com/opencitations/oc_meta/commit/985a2511c600b62719234844626df5791ceb755e))
* xix percentage calculation for provenance statistics to use correct denominator ([f390b8a](https://github.com/opencitations/oc_meta/commit/f390b8aeac70d5cecad8157d7caef0950a66ef44))


### Features

* add benchmark tool for Meta processing pipeline ([c92a5ad](https://github.com/opencitations/oc_meta/commit/c92a5adef874614ac87ba85579bfc0025aba4e7d))
* add convert_citations script for resolving temp IDs to OMIDs ([2bcdca6](https://github.com/opencitations/oc_meta/commit/2bcdca670a8614c78d79903b58fe8f44718d356e))
* add Figshare downloader and refactor Zenodo uploader ([2f8128e](https://github.com/opencitations/oc_meta/commit/2f8128ee6ec34bde9a5a41839c7a59a24f26d30f))
* add random seed support for reproducible benchmark data generation ([a2a0a53](https://github.com/opencitations/oc_meta/commit/a2a0a536ea950e9c46912f5cfb8ecab92269f7f3))
* add script to extract RDF subsets from SPARQL endpoints ([b796bc2](https://github.com/opencitations/oc_meta/commit/b796bc26ee3ceefa52a17353c1f6e703a6f057e7))
* **analyser:** add SPARQL-based dataset analyser ([d10b58a](https://github.com/opencitations/oc_meta/commit/d10b58ad3b0837524687481f2ace5b8a4760369e))
* **benchmark:** add granular curation timing with simplified 2-stack visualization ([f8ca8cd](https://github.com/opencitations/oc_meta/commit/f8ca8cdc1579d8064b0410a579f0da7cd63747e3))
* **benchmark:** add granular storage timing with 3-stack visualization ([5ec6327](https://github.com/opencitations/oc_meta/commit/5ec6327c55650379877335e8da082efc324e7018))
* **benchmark:** add high-author bottleneck reproduction ([d047102](https://github.com/opencitations/oc_meta/commit/d0471023f526541174bc0294df8ece1a3772be7b)), closes [hi#author](https://github.com/hi/issues/author) [--preload-hi#authors](https://github.com/--preload-hi/issues/authors)
* **benchmark:** add preload metrics tracking and remove CSV reading phase ([7f164f0](https://github.com/opencitations/oc_meta/commit/7f164f04f1e6a25523f2a89ba373b29938747e32))
* **benchmark:** add statistical analysis and scalability testing ([22e66e2](https://github.com/opencitations/oc_meta/commit/22e66e29321dbd32942ba88c87be725b89f8afdb))
* **benchmark:** add update scenario ([1d2d821](https://github.com/opencitations/oc_meta/commit/1d2d8215d1409d04b2c6deef6d25a8791a3238fd))
* **benchmark:** add visualization for single-run benchmarks ([2d68d3c](https://github.com/opencitations/oc_meta/commit/2d68d3c4916107a029e28fe56fc5d9319ffcb55b))
* **benchmark:** track peak memory via background RSS sampling ([47b9c9f](https://github.com/opencitations/oc_meta/commit/47b9c9f70a4217fc31c3fbb479d4d1e12a25e98f))
* **benchmark:** unify production and benchmark timing charts ([b2c803e](https://github.com/opencitations/oc_meta/commit/b2c803e302c1158985b9bd5d79b47c347fb26d53))
* **check-rdf-files:** add disk-only verifier for rdf_files_only runs ([e91ee59](https://github.com/opencitations/oc_meta/commit/e91ee59f383e6f4b9a4d1196bc9f1925a4c7b04d))
* **count:** add fast JSON-LD triple counting mode ([53ef90b](https://github.com/opencitations/oc_meta/commit/53ef90bcc5e2dd151ab1d86a933fa597f83c2d5e))
* **csv_generator_lite:** add resume capability via checkpoint system ([46f15bb](https://github.com/opencitations/oc_meta/commit/46f15bb765b186d699d46a3f9a9fefbc62b0c0c9))
* **csv-merge:** introduce light CSV dump merger with streaming approach ([9e5409f](https://github.com/opencitations/oc_meta/commit/9e5409f180298a074806c44bf1f602cd0ae4408b))
* **curator:** add identifiers_only mode for lightweight ID-only curation ([1c487ae](https://github.com/opencitations/oc_meta/commit/1c487ae94ba41e5053e07afc90ff3a4dca858145))
* **download:** add inode monitoring and caching for Virtuoso dumps ([7f0610b](https://github.com/opencitations/oc_meta/commit/7f0610bcf4e520ce662188d6888c1613b8f9e2a7))
* **download:** enhance Virtuoso dump script with auto-organization ([3c7ccd3](https://github.com/opencitations/oc_meta/commit/3c7ccd383cf8a9bfcc060cd6747faf03c74ca980))
* **finder:** add merged entities reconstruction from provenance ([1163da8](https://github.com/opencitations/oc_meta/commit/1163da8cacbb5cced8c08c4f4e0425a45e43a137))
* **fixer:** add hasNext chain anomaly detection and fixer ([0fc9105](https://github.com/opencitations/oc_meta/commit/0fc9105de59a48f5e263882dc69c1887d08c20b6))
* **fixer:** add script to detect identifier schema mismatches ([49a5051](https://github.com/opencitations/oc_meta/commit/49a5051bf7599b4dc6eb153f71fd6f2d465c8508))
* **meta_process:** add CLI flag --timing ([05ff00b](https://github.com/opencitations/oc_meta/commit/05ff00b76d0f9d9646816e7fd2705aa7e67fd770))
* **meta_process:** add parallel bulk loading with automatic cleanup ([50699bf](https://github.com/opencitations/oc_meta/commit/50699bf1278f359559c3533b5ae492eb565ded8d))
* **meta_process:** add Virtuoso bulk loading support ([722a756](https://github.com/opencitations/oc_meta/commit/722a75612aea5fc2a32b7be85d241d3701faac97))
* **meta-process:** update workflow to use triplestore exclusively ([e70f7a2](https://github.com/opencitations/oc_meta/commit/e70f7a2af5254aa638f9c995329c0516723cf750))
* **migration:** add 7z compression option to rdf_to_nquads ([19f8dca](https://github.com/opencitations/oc_meta/commit/19f8dcae245dd4d20861acd3c30f79b48518db65))
* **migration:** add chunked n-quads output ([e22083a](https://github.com/opencitations/oc_meta/commit/e22083afdfc8672de40f5ceca0b77ac279a05b55))
* **migration:** add predicate discovery, graph-less and non-recursive modes to extract_subset ([9f701d1](https://github.com/opencitations/oc_meta/commit/9f701d18afef68331526958d18cb194d2bcc79a1))
* **migration:** add stream_nquads tool ([ad112e1](https://github.com/opencitations/oc_meta/commit/ad112e11a7cbe12f197df6d53d18c42bd05df77a))
* **patch:** add omid mismatch fixer and extract shared matching module ([194327f](https://github.com/opencitations/oc_meta/commit/194327feb81ab044294a6f74c6c7b34dce8bba35))
* **patch:** add script to backfill missing se/1 provenance snapshots ([74f7c43](https://github.com/opencitations/oc_meta/commit/74f7c439be5bba4b5c1e7da4eec2adc88c5d0002))
* **patches:** add script to add xsd:string datatype to untyped literals ([8855466](https://github.com/opencitations/oc_meta/commit/8855466c683efee7777ccb0e73024a43e3597cc4))
* **patches:** add script to fix publication date datatypes ([a0b6ff5](https://github.com/opencitations/oc_meta/commit/a0b6ff574dd10d92d292e2c52b64fe519e2959a2))
* **preprocess_input:** add SPARQL backend and single-file output mode ([334508a](https://github.com/opencitations/oc_meta/commit/334508af62f8a6b602be33d436fbe1c528d76bce))
* **preprocess_input:** drive splitting from meta_config and add per-chunk subfolders ([69ff368](https://github.com/opencitations/oc_meta/commit/69ff3686503d4d62656497fd595a041243168721))
* **preprocess:** add progress bar to deduplication phase ([e2b7c5d](https://github.com/opencitations/oc_meta/commit/e2b7c5d914f4506418550ff2c3a2d07be0264173))
* **preprocess:** make storage checking optional and add configurable parameters ([8353287](https://github.com/opencitations/oc_meta/commit/835328794946771da71302e67d928cff63c53e28))
* **provenance:** add script for JSON-LD to N-Quads conversion ([46c6c3a](https://github.com/opencitations/oc_meta/commit/46c6c3a6dc03de2718bb9e61ceaf03a4081f0449))
* **run:** add Virtuoso dump utilities and quad counter ([3ff2152](https://github.com/opencitations/oc_meta/commit/3ff2152f9208a8e168aa5df42264a0bfa19cc612))
* upgrade to rdflib 7.4.0 and related dependencies ([54bcb4d](https://github.com/opencitations/oc_meta/commit/54bcb4d375a9cf7dfa771496d592091bc3d5e66f))


### Performance Improvements

* benchmark sequential storage and upload mode ([a897f2f](https://github.com/opencitations/oc_meta/commit/a897f2fc392d375d9dae7ba4e42c3217c1276ad6))
* **check_results:** parallelize RDF file checks ([f9a378e](https://github.com/opencitations/oc_meta/commit/f9a378e4901d421975453583b029c7b4f31009e0))
* **check_results:** parallelize SPARQL queries with batched VALUES clauses ([c61c702](https://github.com/opencitations/oc_meta/commit/c61c70259f89d5eeaf253850c249d57604535ae7))
* **check_results:** process csv files sequentially ([3be5cb4](https://github.com/opencitations/oc_meta/commit/3be5cb4822d9b4024cb0145dfe46d72d2e5cd0f9))
* **check_results:** replace string matching with structured parsing and reduce loop passes ([d99491c](https://github.com/opencitations/oc_meta/commit/d99491c149cefed65fa615b65f04abc92fa0a7a5))
* **check_results:** use polars for CSV parsing, parallelize multi-file processing, and fix byte search ([1012c0d](https://github.com/opencitations/oc_meta/commit/1012c0d34e358e719d14e59b1c82cd8bfb699dbd))
* **cleaner:** optimize clean_ra_list from O(n²) to O(n) ([cf3e3e1](https://github.com/opencitations/oc_meta/commit/cf3e3e1d09430b26dbd7d6edc769e52d126d0b65))
* **cleaner:** replace iterative replace() calls with str.translate() ([cfe64e6](https://github.com/opencitations/oc_meta/commit/cfe64e60b4608ab14f11071ed21317360dafa07c))
* **count:** use line counting for nquads and nt formats ([cdd6dc7](https://github.com/opencitations/oc_meta/commit/cdd6dc7e616b7047ea582bb66e188807cfc4bc11))
* **creator:** optimize entity lookup with prebuilt subgraphs ([1db3df8](https://github.com/opencitations/oc_meta/commit/1db3df8b7b44aa2fc586348cbb485da809f2a5db))
* **creator:** optimize indexer_id from O(n×m) to O(n) ([465461b](https://github.com/opencitations/oc_meta/commit/465461bf846334e1a817625efe88b2fbe28c61ce))
* **creator:** replace list.index() with direct index access for role linking ([3d0740c](https://github.com/opencitations/oc_meta/commit/3d0740c951e3b44aadf62f8fc7fbbda4d6c36902))
* **curator:** merge VVI and RA cleaning into single loop ([aaf19b8](https://github.com/opencitations/oc_meta/commit/aaf19b8f83b65a89651d07086b4a9bb9af42798e))
* **curator:** optimize dict access patterns and reduce allocations ([75c2f26](https://github.com/opencitations/oc_meta/commit/75c2f26d915a8504a2d72b7c65a6907b0c78c300))
* **curator:** optimize merge_duplicate_entities from O(n²) to O(n) with index lookup ([cc9f4c1](https://github.com/opencitations/oc_meta/commit/cc9f4c12a67f9f45dcb226f7b38fcfb474449cff))
* **curator:** parallelize identifier collection for large CSVs ([21e6659](https://github.com/opencitations/oc_meta/commit/21e66595d74dd9b0b55c983643fd6f41c300e28d))
* **curator:** replace linear wannabe scans with O(1) index lookups ([96f834d](https://github.com/opencitations/oc_meta/commit/96f834d5b494aab3ad26757e8f1beb13cdc53833))
* **curator:** use set for O(1) deduplication in clean_id_list ([998a977](https://github.com/opencitations/oc_meta/commit/998a977c4f4564221d2e0f52b2bb8097adc8f2a4))
* **curator:** use sets instead of lists for ids and others fields ([1694b48](https://github.com/opencitations/oc_meta/commit/1694b480e562f52d9af30eed47bb28cae5933801))
* **deps:** update oc-ocdm to 11.0.10 with in-memory counter caching ([57dc1f5](https://github.com/opencitations/oc_meta/commit/57dc1f5cbc8f39a51b3235bc3f7d3c522929f185))
* **deps:** update oc-ocdm to 11.0.9, triplelite to 1.4.0, drop virtuoso-utilities ([1382f64](https://github.com/opencitations/oc_meta/commit/1382f64eeb39614faa3dc759e89f29911d057ffb))
* **duplicated_ids_from_files:** implement memory-efficient chunked processing ([095a6c6](https://github.com/opencitations/oc_meta/commit/095a6c68173643f283a920317f11ccbb3e37db98))
* **finder:** batch SPARQL queries per worker to reduce process overhead ([6ec0395](https://github.com/opencitations/oc_meta/commit/6ec0395916101b7ecd6122e4b8c6549347d0cf04))
* **finder:** batch VVI queries with SPARQL VALUES clauses ([5172a26](https://github.com/opencitations/oc_meta/commit/5172a2661a4c576ffb055dbe774157471bca2600))
* **finder:** parallelize SPARQL queries using ProcessPoolExecutor ([46c9b8c](https://github.com/opencitations/oc_meta/commit/46c9b8c88b165d29d4e724df05a2ce3e5329b54b))
* **finder:** replace FILTER with direct literal matching in identifier lookup ([48a023f](https://github.com/opencitations/oc_meta/commit/48a023fb29a67b98bf6c81980f86cebb1cb18866))
* **finder:** replace rdflib Graph with dict-based triple store ([8a1afe7](https://github.com/opencitations/oc_meta/commit/8a1afe77f02b9c0a092059ed0d9b22abdcab070a))
* **finder:** traverse venue children instead of scanning all entities by type ([af000ce](https://github.com/opencitations/oc_meta/commit/af000ceec017e7dcfefa4c170ef87b13f8fb8cf5))
* implement parallel I/O for storage and upload phase ([188f979](https://github.com/opencitations/oc_meta/commit/188f9791a750696f36004853477782b0856cbc66))
* increases SPARQL prefetch parallelism from 1 to 24 workers ([80f29ac](https://github.com/opencitations/oc_meta/commit/80f29ac628b361ae890dbc5be065198e18f14c1b))
* **lib:** add parallel directory traversal for file collection ([4146b9a](https://github.com/opencitations/oc_meta/commit/4146b9a12713c05c8cb4ed0faa20f4023aeb8443))
* **meta_process:** parallelize all operations ([731b65f](https://github.com/opencitations/oc_meta/commit/731b65f0184f17dfc32953d41fa314dcf0641cb0))
* **meta_process:** parallelize SPARQL uploads and update oc-ocdm ([a068aa1](https://github.com/opencitations/oc_meta/commit/a068aa1cc8f42d9f4b4cbcc7cd71eecf21d98cec))
* **meta_process:** process RDF creation in batches to reduce peak memory ([3b3ffae](https://github.com/opencitations/oc_meta/commit/3b3ffae4bd64503ba6278daa925d3db3061a0e60))
* **meta_process:** remove multiprocessing overhead from storage phase ([ff02ea0](https://github.com/opencitations/oc_meta/commit/ff02ea09a337b1a41be11063c3e54b9cf043ab8f))
* **meta:** batch RDF creation and restrict reverse index to queried predicates ([7c68704](https://github.com/opencitations/oc_meta/commit/7c6870414a22100379d933ff330aac3671e47b56))
* **migration:** remove redundant checksum verification from rdf_to_nquads ([a82e975](https://github.com/opencitations/oc_meta/commit/a82e9757ac0c0638430e6cd66c1da3ffc3c01097))
* optimize data structures and reduce unnecessary copies ([16f5b2f](https://github.com/opencitations/oc_meta/commit/16f5b2f5430c648ab0dc8dbcfd676a069835195e))
* optimize group_entities with union-find improvements and batch queries ([ef68932](https://github.com/opencitations/oc_meta/commit/ef68932b2a748cf526eb70993e6d314e1f4405b6))
* optimize venue lookup queries and enhance result checking ([c621534](https://github.com/opencitations/oc_meta/commit/c6215342849b474c72b1b0819e43e0ad159797b6))
* **patches:** optimize add_string_datatype performance ([6a9b2f8](https://github.com/opencitations/oc_meta/commit/6a9b2f84d975aef9689de6163919998f481fb5c1))
* **patch:** parallelize file scanning in fix_misplaced_editor_ars ([5e7db2e](https://github.com/opencitations/oc_meta/commit/5e7db2e8f193d024008bad61790e10b8c13f6e58))
* **preprocess:** parallelize Redis checks with multiprocessing ([aeadabb](https://github.com/opencitations/oc_meta/commit/aeadabbff51f3abcd32b3824de326e50ed25df79))
* **regex:** precompile regex patterns ([dd56692](https://github.com/opencitations/oc_meta/commit/dd566924b628d92944677a35ee5330a79a43ae89))
* **test:** reduce startup latency with exponential backoff ([6be3033](https://github.com/opencitations/oc_meta/commit/6be30339909bf699056ead045115bb8955f17423))
* **test:** simplify reset_server and increase batch size ([58df5b4](https://github.com/opencitations/oc_meta/commit/58df5b45644d7429003c218758efeff782a52f1f))


### BREAKING CHANGES

* output format changed from plain text to JSON
* the generate_rdf_files config key is no longer recognized.
Use rdf_files_only: true to generate only RDF files without triplestore
uploads. The default (false) writes files and uploads.
* The config option `generate_rdf_files` has been renamed to
`rdf_files_only` with inverted semantics. Previously, `generate_rdf_files: true`
meant "also generate RDF files". Now, `rdf_files_only: true` means "generate
only RDF files, skip triplestore updates". The `virtuoso_bulk_load` config
section has been removed entirely.
* removed SPARQL support from preprocess_input.
The --storage-type and --sparql-endpoint options no longer exist.
The --redis-port option is now required (previously defaulted to 6379).
* --predicate and --no-recurse CLI options removed.
Use --entities-file to provide entity URIs from a file instead.
Recursion is now always enabled.

## [2.1.3](https://github.com/opencitations/oc_meta/compare/v2.1.2...v2.1.3) (2025-04-19)


### Bug Fixes

* **ci:** restore test database and fix release workflow ([adde044](https://github.com/opencitations/oc_meta/commit/adde0448b9c25c245a72ff76e6ebf7c524fad5d5))
* **ci:** update system dependency in CI workflow ([b5fe073](https://github.com/opencitations/oc_meta/commit/b5fe073dbb230358c4ba35f7daca176aa12192ab))

## [2.1.2](https://github.com/opencitations/oc_meta/compare/v2.1.1...v2.1.2) (2025-02-06)


### Bug Fixes

* enhance agent role processing in csv generation and improve code quality ([64f0814](https://github.com/opencitations/oc_meta/commit/64f0814ea3db7bf4274cf76599bd96f1e68de1fa))

## [2.1.1](https://github.com/opencitations/oc_meta/compare/v2.1.0...v2.1.1) (2025-02-01)


### Bug Fixes

* handle temporary identifiers deduplication ([258a88f](https://github.com/opencitations/oc_meta/commit/258a88fff4c889308f390caf34531daf7345a187))

# [2.1.0](https://github.com/opencitations/oc_meta/compare/v2.0.3...v2.1.0) (2025-02-01)


### Bug Fixes

* improve VVI query performance by using direct SPARQL ([dd6b728](https://github.com/opencitations/oc_meta/commit/dd6b72818989d476995309ec5cd7b53cbced08ab))


### Features

* add caching and performance improvements to CSV Generator Lite ([491cc1e](https://github.com/opencitations/oc_meta/commit/491cc1ed987845ccacb2de3c6432c80194c1f835))
* Add support for temporary identifiers in meta process ([856c49e](https://github.com/opencitations/oc_meta/commit/856c49e5898c4c3dd3a9bea98ddc4ab0a103ddc4))
* create lightweight CSV generator for basic exports ([fe99bcc](https://github.com/opencitations/oc_meta/commit/fe99bcce47f9c3d645d03852986def7f5569ac56))

## [2.0.3](https://github.com/opencitations/oc_meta/compare/v2.0.2...v2.0.3) (2025-01-22)


### Bug Fixes

* improve container discovery in graph traversal ([45f285c](https://github.com/opencitations/oc_meta/commit/45f285cb8f7e7d1f4ade9396bc054acde795388f))

## [2.0.2](https://github.com/opencitations/oc_meta/compare/v2.0.1...v2.0.2) (2025-01-22)


### Bug Fixes

* prevent duplicate IDs when processing existing identifiers ([88d5b57](https://github.com/opencitations/oc_meta/commit/88d5b57c71cc9b0f074d6b0f4e57cf16ab130634))

## [2.0.1](https://github.com/opencitations/oc_meta/compare/v2.0.0...v2.0.1) (2025-01-20)


### Bug Fixes

* collect IDs from all matches in finder_sparql results ([29fc86c](https://github.com/opencitations/oc_meta/commit/29fc86c66c4ac6e2290a95b1f6c82c14c65594e1))

# 1.0.0 (2025-01-20)


### Bug Fixes

* collect IDs from all matches in finder_sparql results ([b9950b9](https://github.com/opencitations/oc_meta/commit/b9950b9604913a47829f1f4757986a7c62609a7f))
* collect IDs from all matches in finder_sparql results ([6e0fe16](https://github.com/opencitations/oc_meta/commit/6e0fe16e430a812b068ab78fa78c24f5bc91d549))
* improve SPARQL connection handling and error reporting ([e13b13b](https://github.com/opencitations/oc_meta/commit/e13b13b294901647ebd2ad142d6421c602be656e))
* improve SPARQL query retry mechanism ([524dadb](https://github.com/opencitations/oc_meta/commit/524dadb54aa1ff4149c1aa46b91b140134ea2277))
* Improve SPARQL query retry mechanism in ResourceFinder ([d69606f](https://github.com/opencitations/oc_meta/commit/d69606f416cd3e73d0152548658d72ac74755ea8))


### Features

* add script to verify Meta results and duplicate OMID test ([c20d807](https://github.com/opencitations/oc_meta/commit/c20d807c93ab3d20c4ecb187a85a6b024935690e))
