# Leukemia-Cancer-Risk - ETL Project

**Realized by**
* [Valentina Bueno Collazos](https://github.com/valentinabc19)

* [Liseth Esmeralda Erazo Varela](https://github.com/memerazo)

* [Natalia Moreno Montoya](https://github.com/natam226)

## Descripction

This project used a dataset extracted from [Kaggle](https://www.kaggle.com/datasets/ankushpanday1/leukemia-cancer-risk-prediction-dataset?resource=download) containing leukemia-related health data, which includes 143,194 patient records from 22 different countries, with biases in demographic distribution, socioeconomic status, and leukemia prevalence. This dataset has intentional biases to reflect real-world health disparities.

The technologies used are:

- *Python* → Para el análisis exploratorio de datos (EDA) y la limpieza de datos.
- *Jupyter Notebook* → Para la ejecución y documentación del código en Python.
- *PostgreSQL* → Para el almacenamiento y gestión de los datos.
- *PowerBI Desktop* → Para la creación de visualizaciones y dashboards.

The dependencies used in python are in a `requirements.txt` file

## Dataset information

- **patient_id**: ID del paciente, es un número autoincremental.
- **age**: la edad del paciente.
- **gender**: el género del paciente, que podía ser Female (Femenino) o Male (Masculino).
- **country**: país del que es el paciente.
- **wbc_count**: recuente de glóbulos blancos.
- **rbc_count**: recuento de glóbulos rojos.
- **platelet_count**: recuento de plaquetas.
- **hemoglobin_level**: nivel de hemoglobina.
- **bone_marrow_blasts**: Blastos de médula ósea.
- **genetic_mutation**: indica si se tiene alguna mutación genética, se establece con “yes” o “no”.
- **family_history**: indica si se tiene historial familiar de leucemia, se establece con “yes” o “no”.
- **smoking_status**: indica si el paciente fuma o ha fumado, se establece con “yes” o “no”.
- **alcohol_consumption**: indica si el paciente consume o ha consumido, se establece con “yes” o “no”.
- **radiation_exposure**: indica si el paciente ha estado expuesto a radiación, se establece con “yes” o “no”.
- **infection_history**: indica si el paciente tiene un historial de infecciones, se establece con “yes” o “no”.
- **BMI**: es el índice de masa corporal.
- **chronic_illness**: indica si el paciente tiene una enfermedad crónica, se establece con “yes” o “no”.
- **immune_disorders:** indica si el paciente tiene algún desorden inmunológico, se establece con “yes” o “no”.
- **ethnicity**: indica la etnia, como “A”, “B” o “C”.
- **socioeconomic_status**: indica el estado socieconómico del paciente. Se establece como “Medium” (medio), “Low” (bajo) y “High” (alto).
- **urban_rural**: indica si el paciente vive en una zona rural (”Rural”) o urbana (”Urban”).
- **leukemia_status**: indica si el paciente tiene leucemia o no. Se establece como “Negative” (negativo) o “Positive” (positivo).

## Run the project

### Clone the repository

Execute the following command to clone the repository

```bash
git clone https://github.com/valentinabc19/Leukemia-Cancer-Risk-ETL.git

```
> From this point on all processes are done in Visual Studio Code

### Create Virtual Environment
To create a virtual environment you should use the following command
```bash
python -m venv venv
```
To activate the virtual environment you should move to the `venv/Scripts` folder an run the following command to activate the virtual environment
```bash
activate
```

### Credentials
To make a connection to the database you must have the database credentials in a JSON file called credentials. So this file must be created in the project folder, with the following syntax:

```bash
{
    "db_host": "DB_HOST",
    "db_name": "DB_NAME",
    "db_user": "DB_USER",
    "db_password": "DB_PASSWORD",
    "db_port": "DB_PORT"    
}
```

### Installing the dependencies
The necessary dependencies are stored in a file named requirements.txt. To install the dependencies you can use the command
```bash
pip install requirements.txt
```

### Run the notebooks
You execute the two notebooks by the following order:

1. 001_conn_and_data_load.ipynb
2. 002_EDA.ipynb

The correct python kernel must be chosen to run correctly, the correct python kernel is the one stablished in the virtual environment.

### Connecting the database with PowerBI

1. Open PowerBI desktop and create a new file. Select *Get data* and choose the way you want to import the data. In this case is going to be *PostgreSQL Database*.

2. Insert the PostgreSQL server and database name, and press *Accept*.

3. Fill in the fields with the credentials, and press *Accept*.

4. When you're connected to the database you will be able to select the table with the clean data to start creating the dashboard. 
