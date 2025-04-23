#!/usr/bin/env python3
import os
import argparse
import subprocess
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

# Cargar variables de entorno
load_dotenv()

# Configuración
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
S3_BUCKET = os.getenv('S3_BUCKET')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# Inicializar cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=AWS_REGION
)

def create_backup():
    """Crear un backup de la base de datos PostgreSQL y subirlo a S3"""
    try:
        # Crear nombre de archivo con fecha
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_file = f"backup_{DB_NAME}_{timestamp}.sql"
        dump_command = f"pg_dump -h {DB_HOST} -p {DB_PORT} -U {DB_USER} -F c -b -v -f {backup_file} {DB_NAME}"
        
        # Establecer variable de entorno para la contraseña
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_PASSWORD
        
        # Ejecutar pg_dump
        print(f"Creando backup: {backup_file}")
        subprocess.run(dump_command, shell=True, check=True, env=env)
        
        # Subir a S3
        print(f"Subiendo {backup_file} a S3...")
        s3_client.upload_file(backup_file, S3_BUCKET, backup_file)
        print(f"Backup {backup_file} subido exitosamente a S3.")
        
        # Eliminar archivo local
        os.remove(backup_file)
        
    except subprocess.CalledProcessError as e:
        print(f"Error al crear el backup: {e}")
    except NoCredentialsError:
        print("Credenciales de AWS no encontradas o inválidas.")
    except Exception as e:
        print(f"Error inesperado: {e}")

def list_backups():
    """Listar todos los backups disponibles en S3"""
    try:
        print("Backups disponibles en S3:")
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
        
        if 'Contents' not in response:
            print("No se encontraron backups en el bucket.")
            return
            
        backups = sorted([obj['Key'] for obj in response['Contents']], reverse=True)
        for idx, backup in enumerate(backups, 1):
            print(f"{idx}. {backup}")
            
    except ClientError as e:
        print(f"Error al listar backups: {e}")

def restore_backup(backup_name=None, date_str=None):
    """Restaurar un backup desde S3"""
    try:
        if date_str:
            # Buscar backup por fecha aproximada
            response = s3_client.list_objects_v2(Bucket=S3_BUCKET)
            if 'Contents' not in response:
                print("No se encontraron backups en el bucket.")
                return
                
            matching_backups = [
                obj['Key'] for obj in response['Contents'] 
                if date_str in obj['Key']
            ]
            
            if not matching_backups:
                print(f"No se encontraron backups para la fecha: {date_str}")
                return
                
            backup_name = sorted(matching_backups, reverse=True)[0]
            print(f"Seleccionando el backup más reciente que coincide: {backup_name}")
        
        if not backup_name:
            print("Debe especificar un nombre de backup o una fecha.")
            return
            
        # Descargar backup desde S3
        print(f"Descargando {backup_name} desde S3...")
        s3_client.download_file(S3_BUCKET, backup_name, backup_name)
        
        # Restaurar la base de datos
        print(f"Restaurando {backup_name}...")
        
        # Primero terminamos todas las conexiones a la BD
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database="postgres"  # Conectamos a la BD por defecto para poder eliminar la actual
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Terminar conexiones existentes
        cursor.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{DB_NAME}'
            AND pid <> pg_backend_pid();
        """)
        
        # Eliminar y recrear la base de datos
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
        cursor.execute(f"CREATE DATABASE {DB_NAME};")
        cursor.close()
        conn.close()
        
        # Restaurar el backup
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_PASSWORD
        restore_command = f"pg_restore -h {DB_HOST} -p {DB_PORT} -U {DB_USER} -d {DB_NAME} -v {backup_name}"
        subprocess.run(restore_command, shell=True, check=True, env=env)
        
        print("Restauración completada exitosamente.")
        
        # Eliminar archivo local
        os.remove(backup_name)
        
    except OperationalError as e:
        print(f"Error de conexión a PostgreSQL: {e}")
    except subprocess.CalledProcessError as e:
        print(f"Error al restaurar el backup: {e}")
    except ClientError as e:
        print(f"Error al descargar desde S3: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

def main():
    parser = argparse.ArgumentParser(description="Herramienta de Backup/Restauración para PostgreSQL en AWS S3")
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    # Comando para crear backup
    subparsers.add_parser('backup', help='Crear un nuevo backup y subirlo a S3')
    
    # Comando para listar backups
    subparsers.add_parser('list', help='Listar todos los backups disponibles en S3')
    
    # Comando para restaurar backup
    restore_parser = subparsers.add_parser('restore', help='Restaurar un backup desde S3')
    restore_group = restore_parser.add_mutually_exclusive_group(required=True)
    restore_group.add_argument('--name', help='Nombre exacto del backup a restaurar')
    restore_group.add_argument('--date', help='Fecha del backup a restaurar (formato: YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    if args.command == 'backup':
        create_backup()
    elif args.command == 'list':
        list_backups()
    elif args.command == 'restore':
        if args.name:
            restore_backup(backup_name=args.name)
        elif args.date:
            restore_backup(date_str=args.date)

if __name__ == "__main__":
    main()