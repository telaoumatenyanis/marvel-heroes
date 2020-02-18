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

  const heroCollection = db.collection(collectionName);

  let heroes = [];

  // Read CSV file
  fs.createReadStream("./all-heroes.csv")
    .pipe(
      csv({
        separator: ","
      })
    )
    .on("data", async data => {
      const {
        id,
        name,
        imageUrl,
        secretIdentities,
        backgroundImageUrl,
        externalLink,
        description,
        teams,
        powers,
        partners,
        creators,
        birthPlace,
        occupation,
        aliases,
        alignment,
        firstAppearance,
        yearAppearance,
        universe,
        gender,
        type,
        race,
        height,
        weight,
        eyeColor,
        hairColor,
        intelligence,
        strength,
        speed,
        durability,
        power,
        combat
      } = data;
      heroes.push({
        id,
        name,
        imageUrl,
        backgroundImageUrl,
        externalLink,
        description,
        identity: {
          secretIdentities:
            secretIdentities === "" ? [] : secretIdentities.split(","),
          birthPlace: birthPlace,
          occupation: occupation,
          aliases: aliases === "" ? [] : aliases.split(","),
          alignment: alignment,
          firstAppearance: firstAppearance,
          yearAppearance: yearAppearance,
          universe
        },
        appearance: {
          gender: gender,
          type: type,
          race: race,
          height: height,
          weight: weight,
          eyeColor: eyeColor,
          hairColor: hairColor
        },
        teams: teams === "" ? [] : teams.split(","),
        powers: powers === "" ? [] : powers.split(","),
        partners: partners === "" ? [] : partners.split(","),
        creators: creators === "" ? [] : creators.split(","),
        skills: {
          intelligence: parseFloat(intelligence),
          strength: parseFloat(strength),
          speed: parseFloat(speed),
          durability: parseFloat(durability),
          power: parseFloat(power),
          combat: parseFloat(combat)
        }
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
