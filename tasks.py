import os
from celery import Celery
from kombu import Exchange, Queue
from PIL import Image, ImageEnhance, ImageOps

app = Celery("citra", broker="pyamqp://guest@localhost//", backend="rpc://")
OUT = "output"
os.makedirs(OUT, exist_ok=True)

topic_ex = Exchange("image_topic", type="topic")
fanout_ex = Exchange("image_fanout", type="fanout")

app.conf.task_queues = (
    # Topic (routing key persis)
    Queue("gray_q", topic_ex, routing_key="image.filter.grayscale"),
    Queue("tint_q", topic_ex, routing_key="image.color.tint"),
    Queue("sat_q", topic_ex, routing_key="image.color.saturation"),
    # Wildcard
    Queue("color_q", topic_ex, routing_key="image.color.*"),
    Queue("archive_q", topic_ex, routing_key="image.#"),
    # Fan-Out (setiap queue menerima salinan pesan)
    Queue("log_a_q", fanout_ex),
    Queue("log_b_q", fanout_ex),
)

def _simpan(img, path, suffix):
    nama = os.path.splitext(os.path.basename(path))[0]
    hasil = os.path.join(OUT, f"{nama}_{suffix}.png")
    img.save(hasil)
    return hasil

@app.task
def grayscale(path):
    return _simpan(ImageOps.grayscale(Image.open(path)), path, "gray")

@app.task
def tint(path, warna=(255, 0, 0), alpha=0.3):
    img = Image.open(path).convert("RGB")
    overlay = Image.new("RGB", img.size, warna)
    return _simpan(Image.blend(img, overlay, alpha), path, "tint")

@app.task
def saturation(path, faktor=1.8):
    img = Image.open(path).convert("RGB")
    return _simpan(ImageEnhance.Color(img).enhance(faktor), path, "sat")

@app.task
def umumkan(path):
    print(f"[FAN-OUT] gambar baru: {path}")