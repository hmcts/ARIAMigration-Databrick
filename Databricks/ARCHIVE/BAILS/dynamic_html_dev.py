# Databricks notebook source
raw_mnt = "/mnt/ingest00rawsboxraw/ARIADM/ARM/JOH"
landing_mnt = "/mnt/ingest00landingsboxlanding/"
bronze_mnt = "/mnt/ingest00curatedsboxbronze/ARIADM/ARM/JOH"
silver_mnt = "/mnt/ingest00curatedsboxsilver/ARIADM/ARM/JOH"
gold_mnt = "/mnt/ingest00curatedsboxgold/ARIADM/ARM/JOH"

# COMMAND ----------

bails_html_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/template_bail_v0.3.html"
dummy_template_path = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/dummy_template.html"
bail_app_path_1 = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-1.html"
bail_app_path_2 = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-2.html"
bail_app_path_3 = "/dbfs/mnt/ingest00landingsboxhtml-template/Bails/Status-template-3.html"


# COMMAND ----------

with open(bails_html_path, "r") as f:
    bails_html = f.read()
  
with open(dummy_template_path, "r") as f:
    dummy_template = f.read()

with open(bail_app_path_1, "r") as f:
  bail_app_1 = f.read()

with open(bail_app_path_2, "r") as f:
  bail_app_2 = f.read()

for i in range(2):
  if i == 0:
    bail_app_1 = bail_app_1.replace("{{index}}",str(i))
    code += bail_app_1 + "\n"
  if i == 1:
    bail_app_2 = bail_app_2.replace("{{index}}",str(i))
    code += bail_app_2 + "\n"


bails_html = bails_html.replace("{{statusplaceholder}}", code)

displayHTML(bails_html)

# COMMAND ----------


