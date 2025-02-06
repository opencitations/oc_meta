import argparse
import csv
from SPARQLWrapper import SPARQLWrapper, JSON


class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def make_set(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x):
        if x not in self.parent:
            self.make_set(x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]

    def union(self, x, y):
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x != root_y:
            if self.rank[root_x] < self.rank[root_y]:
                self.parent[root_x] = root_y
            elif self.rank[root_x] > self.rank[root_y]:
                self.parent[root_y] = root_x
            else:
                self.parent[root_y] = root_x
                self.rank[root_x] += 1


def find_surviving_entities(results):
    uf = UnionFind()
    for result in results["results"]["bindings"]:
        entity1 = result["entity1"]["value"]
        entity2 = result["entity2"]["value"]
        uf.union(entity1, entity2)

    final_entities = {}
    for entity in uf.parent:
        root = uf.find(entity)
        if root not in final_entities:
            final_entities[root] = set()
        if root != entity:
            final_entities[root].add(entity)

    return final_entities


def execute_sparql_query(endpoint_url, query):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def write_results_to_csv(final_entities, csv_file_path):
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["surviving_entity", "merged_entities"])

        for surviving_entity, merged_entities in final_entities.items():
            if merged_entities:  # Only write rows with actual merges
                writer.writerow([surviving_entity, "; ".join(merged_entities)])

    print(f"CSV file written to {csv_file_path}")


def get_sparql_query(entity_type):
    if entity_type == "id":
        return """
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT ?entity1 ?entity2 {
                ?entity1 datacite:usesIdentifierScheme ?scheme;
                    literal:hasLiteralValue ?literal.
                ?entity2 datacite:usesIdentifierScheme ?scheme;
                    literal:hasLiteralValue ?literal.
                FILTER(?entity1 != ?entity2 )
            }    
        """
    elif entity_type == "ra":
        return """
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT ?entity1 ?entity2 {
                ?entity1 datacite:hasIdentifier ?id;
                        a foaf:Agent.
                ?entity2 datacite:hasIdentifier ?id;
                        a foaf:Agent.
                FILTER(?entity1 != ?entity2 )
            }    
        """
    else:
        raise ValueError(f"Unsupported entity type: {entity_type}")


def debug_specific_entities(results):
    print("Debug: Results for specific entities")
    if not results["results"]["bindings"]:
        print("No results found for the specified entities.")
    for result in results["results"]["bindings"]:
        entity = result.get("entity", {}).get("value", "N/A")
        scheme = result.get("scheme", {}).get("value", "N/A")
        literal = result.get("literal", {}).get("value", "N/A")
        print(f"Entity: {entity}")
        print(f"  Scheme: {scheme}")
        print(f"  Literal: {literal}")
        print("---")


def get_debug_sparql_query():
    return """
    PREFIX datacite: <http://purl.org/spar/datacite/>
    PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>

    SELECT ?entity ?scheme ?literal
    WHERE {
      VALUES ?entity {
        <https://w3id.org/oc/meta/id/061705640086>
        <https://w3id.org/oc/meta/id/061705582835>
      }
      OPTIONAL {
        ?entity datacite:usesIdentifierScheme ?scheme .
      }
      OPTIONAL {
        ?entity literal:hasLiteralValue ?literal .
      }
    }
    """


def main():
    parser = argparse.ArgumentParser(
        description="Run a SPARQL query and save results to CSV."
    )
    parser.add_argument("endpoint", type=str, help="SPARQL endpoint URL")
    parser.add_argument(
        "csv_path", type=str, help="Path to the CSV file to save results"
    )
    parser.add_argument(
        "entity_type",
        type=str,
        choices=["id", "br", "ra"],
        help='Type of entity: "id", "br", or "ra"',
    )
    args = parser.parse_args()

    sparql_query = get_sparql_query(args.entity_type)
    results = execute_sparql_query(args.endpoint, sparql_query)

    # sparql_query = get_debug_sparql_query()
    # results = execute_sparql_query(args.endpoint, sparql_query)

    # debug_specific_entities(results)

    final_entities = find_surviving_entities(results)
    write_results_to_csv(final_entities, args.csv_path)


if __name__ == "__main__":
    main()
