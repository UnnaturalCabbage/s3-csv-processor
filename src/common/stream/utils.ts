import stream from "stream";

export const batch = (batchSize: number) => {
  let buffer: any[] = [];
  return new stream.Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      buffer = (buffer || []).concat(chunk);
      if (buffer.length >= batchSize) {
        this.push(buffer);
        buffer = [];
      }
      callback();
    },
    flush(callback) {
      if (buffer.length > 0) this.push(buffer);
      callback();
    },
  });
};
