const csv = require("csv-parser");
const fs = require("fs");
const { Client } = require("@elastic/elasticsearch");
const heroesIndexName = "heroes";

const MAX_CHUNK_SIZE = 10000;

async function run() {
  // Create Elasticsearch client
  const client = new Client({ node: "http://localhost:9200" });

  // Index creation
  try {
    if (!(await checkIfIndexExists(client, heroesIndexName))) {
      await client.indices.create({
        index: heroesIndexName
      });
      console.log("Created index " + heroesIndexName);
    } else {
      console.log("Index already exists, skipping index creation");
    }
  } catch (err) {
    console.trace(err.message);
  }

  let heroes = [];

  // Read CSV file
  fs.createReadStream("./all-heroes.csv")
    .pipe(
      csv({
        separator: ","
      })
    )
    .on("data", data => {
      heroes.push(data);
      if (heroes.length > MAX_CHUNK_SIZE) {
        client.bulk(createBulkInsertQuery(heroes));
        heroes = [];
      }
    })
    .on("end", async () => {
      try {
        client.bulk(createBulkInsertQuery(heroes));
      } catch (err) {
        console.trace(err);
      } finally {
        client.close();
      }
    });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(heroes) {
  const body = heroes.reduce((acc, hero) => {
    const { id, ...params } = hero;
    acc.push({
      index: { _index: heroesIndexName, _type: "_doc", _id: id }
    });
    acc.push(params);
    return acc;
  }, []);

  return { body };
}

/**
 * Check if an index already exists
 *
 * @param   {any}     client      The elasticsearch client
 * @param   {String}  indexName   The name of the index to check
 *
 * @return  {Boolean}             true if index already exists, false otherwise
 */
async function checkIfIndexExists(client, indexName) {
  return (await client.indices.exists({ index: indexName })).body;
}

run().catch(console.error);
