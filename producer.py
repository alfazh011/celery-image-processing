import os, random, time
from tasks import grayscale, tint, saturation, umumkan

INPUT = "input"
ops = [
    (grayscale, "image.filter.grayscale"),
    (tint,      "image.color.tint"),
    (saturation,"image.color.saturation"),
]

while True:
    f = random.choice(os.listdir(INPUT))
    path = os.path.join(INPUT, f)

    # Fan-Out: umumkan ke semua worker
    umumkan.apply_async(args=[path], exchange="image_fanout", routing_key="")
    print("[FAN-OUT] umumkan", f)

    # Topic/Wildcard: kirim operasi sesuai routing key
    task, key = random.choice(ops)
    task.apply_async(args=[path], exchange="image_topic", routing_key=key)
    print("[TOPIC]  kirim", f, key)
    time.sleep(2)