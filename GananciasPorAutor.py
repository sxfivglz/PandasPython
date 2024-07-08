from ConexionDB import ConexionDB
from ProcesoDatos import Datos
import pandas as pd

if __name__ == "__main__":
    # Parámetros de conexión
    conexion = {
        "host": "localhost",
        "user": "root",
        "password": "",
        "database": "pubs",
        "port": 3307
    }

    # Consulta SQL
    consulta = '''
    SELECT 
        s.ord_num AS Orden,
        au.au_id AS ID,
        au.au_lname AS Apellido,
        au.au_fname AS Nombre,
        t.title_id AS TitleID,
        t.title AS Titulo,
        t.price AS Precio,
        s.qty AS Cantidad,
        COALESCE(ta.royaltyper, 100) AS Regalia
    FROM 
        sales s
        INNER JOIN titles t ON s.title_id = t.title_id
        LEFT JOIN titleauthor ta ON t.title_id = ta.title_id
        LEFT JOIN authors au ON ta.au_id = au.au_id
    '''

    # Conexión y consulta a la base de datos
    db = ConexionDB(**conexion)
    if db.connect():
        data, column_names = db.ejecutarConsulta(consulta)
        db.CerrarConexion()

        if data:
            # Procesamiento de datos con la clase Datos
            datos = Datos(data, column_names)

            datos.convertir_a_numerico(['Precio', 'Cantidad', 'Regalia'])
            datos.calcular_columna('Ganancias', 'Precio * Cantidad')

            df_autores = datos.obtener_datos()
            df_autores = df_autores[df_autores['ID'] != 'NULL']

            total_regalias = df_autores.drop_duplicates(subset=['TitleID', 'ID']).groupby('TitleID')['Regalia'].sum().reset_index()

            df_merged = pd.merge(df_autores, total_regalias, on='TitleID', suffixes=('', '_total'))

            df_merged['Desvio'] = 100 - df_merged['Regalia_total']

            df_merged['GananciasAutor'] = df_merged['Ganancias'] * (df_merged['Regalia'] / 100)
            df_merged['GananciasEditorial'] = df_merged['Ganancias'] * (df_merged['Desvio'] / 100)

            df_ganancias_autores = df_merged.groupby(['Nombre', 'Apellido'])['GananciasAutor'].sum().reset_index()

            df_editorial_porcentaje = df_merged['GananciasEditorial'].sum()
            df_ganancia_editorial = df_merged[df_merged['ID'].isnull()]['Ganancias'].sum()

            ganancias_editorial_final = df_editorial_porcentaje + df_ganancia_editorial
            df_ganancias_editorial = pd.DataFrame({
                'Nombre': ['Desconocido'],
                'Apellido': ['Desconocido'],
                'GananciasAutor': [ganancias_editorial_final]
            })

            gananciasporautor = pd.concat([df_ganancias_autores, df_ganancias_editorial])
            
            # Ajuste de configuración de pandas para la impresión
            pd.set_option('display.max_colwidth', None)
            pd.set_option('display.expand_frame_repr', False)
            pd.set_option('display.unicode.east_asian_width', True)

            print("«" * 50)
            print("Ganancias por Autor")
            print(gananciasporautor.to_string(index=False, justify='center'))
            print("»" * 50)
            datos.exportar_a_csv('ganancias_por_autor.csv', gananciasporautor)
