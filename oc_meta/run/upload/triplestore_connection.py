from SPARQLWrapper import SPARQLWrapper, POST
from typing import Optional

class TriplestoreConnection:
    _instance: Optional['TriplestoreConnection'] = None
    _sparql: Optional[SPARQLWrapper] = None

    def __new__(cls, endpoint_url: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(TriplestoreConnection, cls).__new__(cls)
            if endpoint_url:
                cls._instance._init_connection(endpoint_url)
        elif endpoint_url:
            # Se viene fornito un nuovo URL e l'istanza esiste già, aggiorna la connessione
            cls._instance._init_connection(endpoint_url)
        return cls._instance

    def _init_connection(self, endpoint_url: str) -> None:
        """Inizializza la connessione al triplestore"""
        self._sparql = SPARQLWrapper(endpoint_url)
        self._sparql.setMethod(POST)

    @property
    def sparql(self) -> SPARQLWrapper:
        """Restituisce l'istanza di SPARQLWrapper"""
        if self._sparql is None:
            raise RuntimeError("Connection not initialized. Provide endpoint_url when creating instance.")
        return self._sparql

    def execute_update(self, query: str) -> bool:
        """
        Esegue una query di update sul triplestore
        
        Args:
            query (str): Query SPARQL da eseguire
            
        Returns:
            bool: True se l'esecuzione ha successo, False altrimenti
        """
        try:
            self.sparql.setQuery(query)
            self.sparql.queryAndConvert()
            return True
        except Exception as e:
            print(f"Error executing query: {e}")
            return False 