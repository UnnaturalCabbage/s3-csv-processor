import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";
import S3ChunkStream from "./S3ChunkStream";

export const objectExists = async (
  region: string,
  bucket: string,
  key: string
): Promise<boolean> => {
  const client = new S3Client({
    region,
  });
  const command = new HeadObjectCommand({
    Bucket: bucket,
    Key: key,
  });
  const exists = await client
    .send(command)
    .then(() => true)
    .catch(() => false);
  return exists;
};

export const readObject = (
  region: string,
  bucket: string,
  key: string
): S3ChunkStream => {
  return new S3ChunkStream({
    s3Client: new S3Client({
      region,
      maxAttempts: 5,
    }),
    bucket,
    key,
  });
};

export const parseObjectUrl = (
  url: string
): { bucket: string; key: string; region: string } | null => {
  const isHttp = url.startsWith("https://") || url.startsWith("http://");

  if (isHttp) {
    const urlObj = new URL(url);
    const host = urlObj.hostname;
    const parts = host.split(".");
    const region = parts[1];
    const pathParts = urlObj.pathname.split("/").filter(Boolean);
    const bucket = pathParts[0];
    const key = pathParts.slice(1).join("/");

    if (!bucket || !key || !region) return null;

    return { bucket, key, region };
  }

  return null;
};

export { S3ChunkStream };
