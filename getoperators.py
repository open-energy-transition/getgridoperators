import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

ENDPOINT_URL = "https://query.wikidata.org/sparql"

sparql = SPARQLWrapper(ENDPOINT_URL)
sparql.setReturnFormat(JSON)

query = """
SELECT
  ?operator ?operatorLabel
  ?country ?countryLabel
  ?website
  ?hqLabel
WHERE {
  ?operator wdt:P31 wd:Q112046 ;
            wdt:P17 ?country .

  OPTIONAL { ?operator wdt:P856 ?website . }
  OPTIONAL { ?operator wdt:P159 ?hq . }

  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en".
  }
}
ORDER BY ?countryLabel ?operatorLabel
"""

sparql.setQuery(query)
results = sparql.query().convert()["results"]["bindings"]

rows = []

for r in results:
    rows.append({
        "operator_qid": r["operator"]["value"].split("/")[-1],
        "operator_label": r["operatorLabel"]["value"],
        "country_qid": r["country"]["value"].split("/")[-1],
        "country_label": r["countryLabel"]["value"],
        "website": r.get("website", {}).get("value"),
        "headquarters": r.get("hqLabel", {}).get("value")
    })

df = pd.DataFrame(rows)
df.to_csv("grid_operators_worldwide.csv", index=False)

print(f"✅ Done. Retrieved {len(df)} operators.")
