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
            secretIdentities.length === 0 ? [] : secretIdentities.split(","),
          birthPlace: birthPlace,
          occupation: occupation,
          aliases: aliases.length === 0 ? [] : aliases.split(","),
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
        teams: teams.length === 0 ? [] : teams.split(","),
        powers: powers.length === 0 ? [] : powers.split(","),
        partners: partners.length === 0 ? [] : partners.split(","),
        creators: creators.length === 0 ? [] : creators.split(","),
        skills: {
          intelligence: formatSkill(intelligence),
          strength: formatSkill(strength),
          speed: formatSkill(speed),
          power: formatSkill(power),
          combat: formatSkill(combat)
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

formatSkill = skill =>
  !isNaN(skill) && skill.length !== 0 ? parseFloat(skill) : 0;

run().catch(console.error);
