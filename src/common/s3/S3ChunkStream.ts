import Stream, { Readable } from "stream";
import {
  GetObjectCommand,
  S3Client,
  GetObjectCommandOutput,
} from "@aws-sdk/client-s3";

const oneMB = 1024 * 1024;

interface RangeAndLength {
  start: number;
  end: number;
  length: number;
}

export interface S3ChunkStreamOptions {
  s3Client: S3Client;
  bucket: string;
  key: string;
}

class S3ChunkStream extends Readable {
  private bucket: string;
  private key: string;
  private s3Client: S3Client;
  private rangeAndLength: RangeAndLength = { start: -1, end: -1, length: -1 };

  constructor(
    options: S3ChunkStreamOptions,
    streamOptions?: Stream.ReadableOptions
  ) {
    super(streamOptions);
    this.bucket = options.bucket;
    this.key = options.key;
    this.s3Client = options.s3Client;
  }

  _read(): void {
    this.fetchChunk().catch((err) => this.destroy(err));
  }

  private async fetchChunk(): Promise<void> {
    if (this.isComplete()) {
      this.push(null);
      return;
    }

    const nextStart = this.rangeAndLength.end + 1;
    const nextEnd = this.rangeAndLength.end + oneMB * 2;

    const response: GetObjectCommandOutput = await this.getObjectRange(
      nextStart,
      nextEnd
    );

    if (!response.ContentRange) {
      throw new Error("Missing ContentRange in response");
    }
    this.rangeAndLength = this.getRangeAndLength(response.ContentRange);

    if (!response.Body) {
      throw new Error("Missing Body in response");
    }

    const chunk = await (response.Body as any).transformToByteArray();
    this.push(Buffer.from(chunk));
  }

  private async getObjectRange(
    start: number,
    end: number
  ): Promise<GetObjectCommandOutput> {
    const command = new GetObjectCommand({
      Bucket: this.bucket,
      Key: this.key,
      Range: `bytes=${start}-${end}`,
    });
    return await this.s3Client.send(command);
  }

  private getRangeAndLength(contentRange: string): RangeAndLength {
    const [rangePart, totalLength] = contentRange.split("/");
    const rangeStr = rangePart.replace("bytes ", "");
    const [startStr, endStr] = rangeStr.split("-");
    return {
      start: parseInt(startStr, 10),
      end: parseInt(endStr, 10),
      length: parseInt(totalLength, 10),
    };
  }

  private isComplete(): boolean {
    return this.rangeAndLength.end === this.rangeAndLength.length - 1;
  }
}

export default S3ChunkStream;
