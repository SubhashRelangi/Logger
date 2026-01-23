import time
from pathlib import Path


def read_exact(f, n):
    data = f.read(n)
    if len(data) != n:
        raise EOFError
    return data


def convert_bin_to_csv(bin_path, csv_path):
    start_time = time.perf_counter()

    with open(bin_path, "rb") as f, open(csv_path, "w", encoding="utf-8") as out:

        # ---------- HEADER ----------
        magic = read_exact(f, 4)
        if magic != b"LOG1":
            raise ValueError("Invalid binary log format")

        version = int.from_bytes(read_exact(f, 1), "little")
        field_count = int.from_bytes(read_exact(f, 1), "little")

        schema = []
        for _ in range(field_count):
            name_len = int.from_bytes(read_exact(f, 1), "little")
            name = read_exact(f, name_len).decode("utf-8")
            schema.append(name)

        # write CSV header
        out.write(",".join(schema) + "\n")

        # ---------- RECORDS ----------
        record_count = 0

        while True:
            try:
                payload_len = int.from_bytes(read_exact(f, 2), "little")
                payload = read_exact(f, payload_len)
            except EOFError:
                break

            values = []
            offset = 0

            for _ in range(field_count):
                val_len = int.from_bytes(payload[offset:offset+2], "little")
                offset += 2
                val = payload[offset:offset+val_len].decode("utf-8")
                offset += val_len
                values.append(val)

            out.write(",".join(values) + "\n")
            record_count += 1

    elapsed = time.perf_counter() - start_time

    print(f"Converted {record_count} records")
    print(f"Time taken: {elapsed:.6f} seconds")
    print(f"Throughput: {record_count / elapsed:.2f} records/sec")


def main():
    Bin_Path = "/home/user1/learning/LoggerModule/logs8/log_20260120_164809_160.bin"
    Csv_Path = "/home/user1/learning/LoggerModule/csv_logs/log_20260120_164809_160.csv"

    convert_bin_to_csv(Bin_Path, Csv_Path)

if __name__ == "__main__":
    main()
