import os, json, socket, uuid, tempfile, time, importlib
from typing import List
import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
from scripts.utils.env_loader import load_env_file


@pytest.mark.integration
def test_current_aqi_to_kafka_integration(monkeypatch):
  
  # 1) ENV de test
  tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
  monkeypatch.setenv("ENV_FILE", tests_env)
  load_env_file()

  broker = os.getenv("KAFKA_BROKER")
  topic  = os.getenv("TOPIC_AQI_RAW")
  suffix = os.getenv("nombre")
  assert broker, "KAFKA_BROKER doit être défini dans tests/.env_test"

  # 2) Reachability Kafka -> FAIL si mauvais broker
  host, port = broker.split(":")
  try:
      with socket.create_connection((host, int(port)), timeout=5):
          pass
  except Exception as e:
      pytest.fail(f"KAFKA_BROKER injoignable ({broker}) : {e}")

  # 3) Préparer topic et cities.json temporaires
  suffix = uuid.uuid4().hex[:8]
  topic = f"it_aqi_current_{suffix}"

  cities_data = [
      {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
      {"name": "Lyon",  "lat": 45.7640, "lon": 4.8357},
  ]
  tmp_dir = tempfile.TemporaryDirectory()
  cities_path = os.path.join(tmp_dir.name, "cities.json")
  with open(cities_path, "w", encoding="utf-8") as f:
      json.dump(cities_data, f)

  # 4) Créer le topic éphémère
  admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_current_admin")
  try:
      if topic not in set(admin.list_topics()):
          admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
  finally:
      admin.close()

  # 5) Importer le module après l’ENV, PUIS forcer ses globals
  from scripts.etl import current_aqi_to_kafka as mod
  importlib.reload(mod)
  mod.KAFKA_BROKER = broker
  mod.TOPIC_AQI = topic
  mod.CITIES_FILE = cities_path

  # --- Faux client Open-Meteo (pas d’appel réseau) -----------------
  class _Var:
      def __init__(self, v): self.v = v
      def Value(self): return self.v

  class _Cur:
      def __init__(self, t, interval, vals): self._t=t; self._i=interval; self._vals=vals
      def Time(self): return self._t
      def Interval(self): return self._i
      def Variables(self, i): return _Var(self._vals[i])

  class _Resp:
      def __init__(self, lat, lon, t, interval, vals):
          self._lat=lat; self._lon=lon; self._cur=_Cur(t, interval, vals)
      def Latitude(self): return self._lat
      def Longitude(self): return self._lon
      def Current(self): return self._cur

  def fake_weather_api(url, params):
      n = len(mod.AQI_VARS)
      return [_Resp(params["latitude"], params["longitude"], 1_700_000_000, 3600, [42.0]*n)]

  mod.openmeteo.weather_api = fake_weather_api
  # -----------------------------------------------------------------

  # 6) Produire les messages
  run_id = f"run_{suffix}"
  sent = mod.push_current_aqi_to_kafka(run_id=run_id)
  assert sent == len(cities_data)

  # 7) Consommer et vérifier
  consumer = KafkaConsumer(
      topic,
      bootstrap_servers=broker,
      auto_offset_reset="earliest",
      enable_auto_commit=False,
      group_id=f"it_consumer_{suffix}",
    #   group_id=None,
      value_deserializer=lambda m: json.loads(m.decode("utf-8")),
      consumer_timeout_ms=5000,
  )
  try:
      msgs = [m.value for m in consumer]
  finally:
      consumer.close()

  msgs = [m for m in msgs if m.get("run_id") == run_id]
  assert len(msgs) == len(cities_data)

  for m in msgs:
      for key in ["city","latitude","longitude","time","interval","european_aqi","pm2_5","pm10"]:
          assert key in m

  # 8) Cleanup
  # admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_current_admin_cleanup")
  # try:
  #     admin.delete_topics([topic])
  # finally:
  #     admin.close()

  admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_cleanup")
  try:
      admin.delete_topics([topic])
      # attendre que le topic disparaisse
      for _ in range(20):
          if topic not in set(admin.list_topics()):
              break
          time.sleep(0.5)
  finally:
      admin.close()
