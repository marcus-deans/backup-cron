import { exec } from "child_process";
import { PutObjectCommand, S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { createReadStream } from "fs";

import { env } from "./env";

const { Node: Logtail } = require("@logtail/js");
const logtail = new Logtail("6nRxM8PA3N6czsSEz38dXHKr");


const uploadToS3 = async ({ name, path }: {name: string, path: string}) => {
  console.log("Uploading backup to S3...");
  logtail.info("Uploading DB backup to S3");

  const bucket = env.AWS_S3_BUCKET;

  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
  }

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`)
    clientOptions['endpoint'] = env.AWS_S3_ENDPOINT;
  }

  const client = new S3Client(clientOptions);

  try {
    const data = await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: name,
        Body: createReadStream(path),
      })
    );
    return data;
    logtail.info("DB backup uploaded to S3");
  } catch (err){
    logtail.error("Error uploading DB backup to S3", {
      error: err
    })
    console.log("Error", err);
  }
}

const dumpToFile = async (path: string) => {
  console.log("Dumping DB to file...");
  logtail.info("Dumping DB to file");

  await new Promise((resolve, reject) => {
    exec(
      `pg_dump ${env.BACKUP_DATABASE_URL} -F t | gzip > ${path}`,
      (error, stdout, stderr) => {
        if (error) {
          reject({ error: JSON.stringify(error), stderr });
          return;
        }

        resolve(undefined);
      }
    );
  });

  logtail.info("DB dumped to file");
  console.log("DB dumped to file...");
}

export const backup = async () => {
  let date = new Date().toISOString()
  const timestamp = date.replace(/[:.]+/g, '-')

  logtail.info(`Initiating DB backup`, {
    timestamp: timestamp,
    type: `${env.BACKUP_FILEPATH_PREFIX}`
  })

  const filename = `backup-${timestamp}.tar.gz`
  const filepath = `/tmp/${env.BACKUP_FILEPATH_PREFIX}/${filename}`

  await dumpToFile(filepath)
  await uploadToS3({name: filename, path: filepath})

  logtail.info(`DB backup complete`);
  logtail.flush()
}
