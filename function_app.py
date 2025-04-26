import logging, os, datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Trigger at 2:00 AM daily
@app.timer_trigger(schedule="0 0 2 * * *", arg_name="myTimer")
def ingestTimerTrigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("Ingestion timer is past due!")

    # today’s partition
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    # connect to your storage account
    conn_str = os.environ["AzureWebJobsStorage"]
    svc = BlobServiceClient.from_connection_string(conn_str)
    raw_ct = svc.get_container_client("rawdata")
    landing_ct = svc.get_container_client("landing")

    for blob_name in ["400_transactions.csv", "400_households.csv", "400_products.csv"]:
        # download the raw blob
        data = raw_ct.download_blob(blob_name).readall()
        # upload it under landing/YYYY-MM-DD/
        dest_path = f"{today}/{blob_name}"
        landing_ct.upload_blob(name=dest_path, data=data, overwrite=True)
        logging.info(f"Copied {blob_name} → landing/{dest_path}")

    logging.info("Ingestion completed.")

@app.timer_trigger(schedule="0 0 3 * * 0", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def RetrainTimerTrigger(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
