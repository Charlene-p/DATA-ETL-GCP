def import_bq_table(bq_table_path):
    bq_table = bigquery.TableReference.from_string(
    bq_table_path
    )
    rows = bq_client.list_rows(
        bq_table
    )
    df = rows.to_dataframe( 
        create_bqstorage_client=True,
    )
    return df

def export_to_bq(df, bq_table, bq_dataset = 'CMP'):
    bq_table_path = bq_client.dataset(bq_dataset).table(bq_table)
    bq_client.delete_table(bq_table_path, not_found_ok=True)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, bq_table_path, job_config)
    print("Export {} to BigQuery.".format(bq_table))
    return 'Done'

def export_to_gcs(df, bucket_name, directory_name, df_name):
    bucket = gcs_client.get_bucket(bucket_name)
    today = str(datetime.today().date()).replace("-", "_")
    file = '{}_{}.csv'.format(df_name, today)
    bucket.blob(directory_name + file).upload_from_string(df.to_csv(), 'csv')
    print("Export {} to GCS done!".format(file))
    return 'Done'

def query_bigquery(sql):
    data = bq_client.query(sql).to_dataframe()
    return data
                                                        
def meta_data_bq_to_gcs(project, dataset_id, bucket_name, temp_dir, output_dir, table_id ):
    today = str(datetime.today().date()).replace("-", "_")

    destination_uri = "gs://{}/{}/{}*.csv".format(bucket_name, temp_dir, table_id)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table("{}*".format(table_id))
    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        location="US",
    ) 
    extract_job.result()  # Waits for job to complete.
    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id , destination_uri)
    )


    bucket = gcs_client.get_bucket(bucket_name)
    destination_blob_name = "{}/{}_{}.csv".format(output_dir , table_id, today)
    destination = bucket.blob(destination_blob_name)
    destination.content_type = "text/csv"
    print(destination)
    direc = '{}/'.format(temp_dir)
    compose_blobs = bucket.list_blobs(prefix=direc)
    print(compose_blobs)
    sources = [b for b in compose_blobs if table_id in b.name]
    print(sources)
    destination.compose(sources)

    for blob in sources:
        blob.delete()
        print("Delet:" + str(blob.name.split("/")[-1]))
    return done
