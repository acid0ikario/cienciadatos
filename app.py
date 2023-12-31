import glob
import shutil
import unicodedata
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, coalesce, lit, col, udf, when, to_date, date_format, round
from datetime import datetime
import time

DECIMAL_TYPE = "decimal(10, 2)"
def remove_accents(df):
    for column in df.columns:
        strip_accents_udf = udf(lambda x: unicodedata.normalize('NFD', x).encode('ascii', 'ignore').decode('utf-8') if x is not None else x)
        df = df.withColumn(column, strip_accents_udf(col(column))) 
    return df
   
def rename_columns(df):
    new_columns = [col(c).alias(c.replace(" ", "_")) for c in df.columns]
    return df.select(*new_columns)

def process_csv(input_file, output_file):
    # Nombre del archivo CSV con la fecha
    output_file = "/tmp/transformed"
    result_output_file = "/tmp/result/ventasprocesadas.csv"
    # Inicializa una sesión de Spark
    spark = SparkSession.builder.appName("Transformación de Datos").getOrCreate()

    # Carga el archivo CSV en un DataFrame de Spark
    df = spark.read.csv("/tmp/ventas.csv", header=True, inferSchema=True)
    df = rename_columns(df)

    # Remove rows with null or empty values in the cli_NCLIENTE column
    df = df.na.drop(subset=["cli_NCLIENTE"])
    df = df.na.drop(subset=["clas_Clasificacion"])
    df = df.na.drop(subset=["clas_GRUPO"])

    # Crea una nueva columna "monto_F_ABS" con los valores absolutos de "tranc_Monto_F"
    df = df.withColumn("monto_F_ABS", abs(df["tranc_Monto_F"]))

    # Reemplaza valores nulos o vacíos en la columna "cli_CODCLIENTE" con "000-000"
    df = df.withColumn("cli_CODCLIENTE", coalesce(df["cli_CODCLIENTE"], lit("000-000")))

    df = df.withColumn('Beneficios', df['tranc_Monto_F'] - df['pro_Costo_del_articulo'])
    df = df.withColumn('Perdidas', when(df['pro_Costo_del_articulo'] - df['tranc_Monto_F'] < 0, 0).otherwise(df['pro_Costo_del_articulo'] - df['tranc_Monto_F']))
    df = df.withColumn('Rentabilidad', (df['Beneficios'] / df['tranc_Monto_F']) * 100)
    df = df.withColumn('Ingresos_Generados', df['tranc_Monto_F'] - df['tranc_Descuento_total'])


    df = df.withColumn('Margen_de_Beneficios', (df['Beneficios'] / df['Ingresos_Generados']) * 100)
    df = df.withColumn('Monto_Total_con_Descuento', df['tranc_Monto_F'] - df['tranc_Descuento_total'])
    df = df.withColumn("tranc_Fecha_de_contabilizacion", date_format(to_date("tranc_Fecha_de_contabilizacion", "dd-MM-yy"), "dd/MM/yyyy"))
    df = df.withColumn("tranc_QUANTITY_F", coalesce(round(col("tranc_QUANTITY_F"), 2), lit(0)))



    df = df.withColumn("Perdidas", df["Perdidas"].cast(DECIMAL_TYPE))
    df = df.withColumn("pro_MARGEN", df["pro_MARGEN"].cast(DECIMAL_TYPE))
    df = df.withColumn("pro_Costo_del_articulo", df["pro_Costo_del_articulo"].cast(DECIMAL_TYPE))
    df = df.withColumn("tranc_Descuento_total", df["tranc_Descuento_total"].cast(DECIMAL_TYPE))
    df = df.withColumn("Monto_Total_con_Descuento", df["Monto_Total_con_Descuento"].cast(DECIMAL_TYPE))
    df = df.withColumn("Margen_de_Beneficios", df["Margen_de_Beneficios"].cast(DECIMAL_TYPE))
    df = df.withColumn("Ingresos_Generados", df["Ingresos_Generados"].cast(DECIMAL_TYPE))
    df = df.withColumn("Rentabilidad", df["Rentabilidad"].cast(DECIMAL_TYPE))
    df = df.withColumn("Beneficios", df["Beneficios"].cast(DECIMAL_TYPE))
    df = df.withColumn("tranc_Monto_F", df["tranc_Monto_F"].cast(DECIMAL_TYPE))
    df = df.withColumn("pro_Precio_tras_el_descuento", df["pro_Precio_tras_el_descuento"].cast(DECIMAL_TYPE))

    # Guarda el DataFrame transformado en un solo archivo CSV
    df.coalesce(1).write.csv(output_file, header=True, mode="overwrite", sep=";")

    # Copia el último archivo generado a la carpeta /tmp/result con el nombre ventasprocesadas.csv
    latest_file = max(glob.glob(f"{output_file}/*.csv"), key=os.path.getctime)
    os.makedirs(os.path.dirname(result_output_file), exist_ok=True)
    shutil.copy(latest_file, result_output_file)
    
    latest_file = max(glob.glob(f"{output_file}/*.csv"), key=os.path.getctime)
    result_output_file = "/tmp/result/ventasprocesadas.csv"
    os.makedirs(os.path.dirname(result_output_file), exist_ok=True)
    shutil.copy(latest_file, result_output_file)

    # Cierra la sesión de Spark
    spark.stop()
# Obtén la fecha actual en el formato deseado (puedes ajustar el formato según tus preferencias)
current_date = datetime.now().strftime("%Y-%m-%d-%H-%M")

input_file = "/tmp/ventas.csv"
output_folder = "/tmp/transformed"
result_output_file = "/tmp/result/ventasprocesadas.csv"
processed_folder = "/tmp/procesados"
while True:
    if os.path.exists(input_file):
        # Process the CSV file
        process_csv(input_file, output_folder)

        # Move the processed file to the "procesados" folder
        shutil.move(input_file, os.path.join(processed_folder, f'ventas_{current_date}.csv'))
        
        print(f"Processed file: {input_file}")
    else:
        print(f"No file found: {input_file}")

    # Sleep for some time before checking for a new file again
    time.sleep(3)  # Sleep for 60 seconds, you can adjust this as needed