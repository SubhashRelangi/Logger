import time
import datetime
import struct
from logger import Logger



def realtime_hms_ms():
    now = datetime.datetime.now()
    return now.strftime("%H:%M:%S") + f":{now.microsecond // 1000:03d}"


# def time_to_ms(t: str) -> int:
#     h, m, s, ms = map(int, t.split(":"))
#     return ((h * 3600 + m * 60 + s) * 1000) + ms


# ---------------- TLV helpers ----------------

# def tlv(tag: int, value: bytes) -> bytes:
#     """
#     TLV = [TAG:uint8][LEN:uint16][VALUE]
#     """
#     if not (0 <= tag <= 255):
#         raise ValueError("TLV tag must fit uint8")

#     length = len(value)
#     if length > 0xFFFF:
#         raise ValueError("TLV length overflow")

#     return struct.pack("<BH", tag, length) + value


# def build_tlv_record(ts_ms: int, values: list[float], sat_count: int) -> bytes:
#     record = b""

#     record += tlv(1, struct.pack("<I", ts_ms))

#     record += tlv(2, struct.pack("<f", values[0]))
#     record += tlv(3, struct.pack("<f", values[1]))

#     record += tlv(4, struct.pack("<f", values[2]))
#     record += tlv(5, struct.pack("<f", values[3]))
#     record += tlv(6, struct.pack("<f", values[4]))

#     record += tlv(7, struct.pack("<f", values[5]))
#     record += tlv(8, struct.pack("<f", values[6]))
#     record += tlv(9, struct.pack("<f", values[7]))

#     record += tlv(10, struct.pack("<B", sat_count))

#     record += tlv(11, struct.pack("<f", values[8]))
#     record += tlv(12, struct.pack("<f", values[9]))
#     record += tlv(13, struct.pack("<f", values[10]))

#     return record



def main():
    logger = Logger()
    logger.initialize("csv", compress=True)

    logger.headers(
        "timestamp",
        "degrees0", "degrees1",
        "volts0", "amps0", "watts0",
        "volts1", "amps1", "watts1",
        "satellite_count",
        "heading",
        "latitude", "longitude"
    )

    logger.start()

    # ts_ms = time_to_ms(realtime_hms_ms())  # constant timestamp (test case)

    # values = [
    #     195.029296875,
    #     314.12109375,
    #     24.0538711547852,
    #     -0.014181817881763,
    #     -0.238541349768639,
    #     -0.006679157260805,
    #     0.018545454367995,
    #     -0.079513780772686,
    #     233.245483398438,
    #     22.5792655944824,
    #     75.7095489501953,
    # ]

    # sat_count = 9

    # # # TLV Format
    # binary_data = build_tlv_record(ts_ms, values, sat_count)

    # packer = struct.Struct("<I 11f B")

    # # BIN Format
    # binary_data = packer.pack(
    #     ts_ms,
    #     *values,
    #     sat_count
    # )

    start = time.time()
    sec = 0

    try:
        while True:
            pref = time.time()

            logger.publish([
                realtime_hms_ms(),
                "195.029296875", "314.12109375",
                "24.0538711547852", "-0.014181817881763", "-0.238541349768639",
                "-0.006679157260805", "0.018545454367995", "-0.079513780772686",
                "9",
                "233.245483398438",
                "22.5792655944824",
                "75.7095489501953"
            ])
            # logger.publish(binary_data)

            sec += 1
            if pref - start >= 1.0:
                print(f"[Main] Exc: {sec}")
                sec = 0
                start = pref

            # time.sleep(0.001)

    except KeyboardInterrupt:
        print("\n[Main] Ctrl+C detected. Stopping logger...")

    finally:
        logger.stop()
        print("[Main] Logger stopped safely")


if __name__ == "__main__":
    main()
