var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

const MAX_CHUNK_SIZE = 10000;

async function run() {
  // Create Elasticsearch client
  const client = await MongoClient.connect(mongoUrl);

  const db = client.db(dbName);

  const heroCollection = db.collection("heroes");

  let heroes = [];

  // Read CSV file
  fs.createReadStream("./all-heroes.csv")
    .pipe(
      csv({
        separator: ","
      })
    )
    .on("data", async data => {
      const { teams, powers, partners, creators } = data;

      heroes.push({
        ...data,
        teams: teams === "" ? [] : teams.split(","),
        powers: powers === "" ? [] : powers.split(","),
        partners: partners === "" ? [] : partners.split(","),
        creators: creators === "" ? [] : creators.split(",")
      });
      if (heroes.length > MAX_CHUNK_SIZE) {
        await heroCollection.insertMany();
        heroes = [];
      }
    })
    .on("end", async () => {
      try {
        await heroCollection.insertMany(heroes);
      } catch (err) {
        console.trace(err);
      } finally {
        client.close();
      }
    });
}

run().catch(console.error);
