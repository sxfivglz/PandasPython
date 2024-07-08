import pandas as pd

class Datos:
    def __init__(self, datos=None, columnas=None):
        if datos is not None and columnas is not None:
            self.df = pd.DataFrame(datos, columns=columnas)
        else:
            self.df = pd.DataFrame()

    def cargar_datos(self, datos, columnas):
        self.df = pd.DataFrame(datos, columns=columnas)

    def exportar_a_csv(self, nombre_archivo, df=None):
        df_a_exportar = df if df is not None else self.df
        try:
            df_a_exportar.to_csv(nombre_archivo, index=True, header=True, sep=',', encoding='utf-8')
            print(f"Datos exportados correctamente a {nombre_archivo}")
        except Exception as e:
            print(f"Error al exportar los datos: {e}")

    def convertir_a_numerico(self, columnas):
        for columna in columnas:
            self.df[columna] = pd.to_numeric(self.df[columna], errors='coerce')

    def calcular_columna(self, new_column_name, operation):
        try:
            self.df[new_column_name] = self.df.eval(operation)
        except Exception as e:
            print(f"Error al calcular la columna {new_column_name}: {e}")

    def agrupar_sumar(self, group_by_column, sum_column):
        if group_by_column and sum_column:
            grouped_df = self.df.groupby(group_by_column)[sum_column].sum().reset_index()
            return grouped_df.sort_values(by=sum_column, ascending=False).reset_index(drop=True)
        else:
            print("No se proporcionaron columnas válidas para agrupar y sumar.")
            return pd.DataFrame()

    def rankear_mayores(self, group_by_column, rank_column, method):
        try:
            self.df[f'{rank_column}'] = self.df.groupby(group_by_column)[rank_column].rank(method=method, ascending=False)
        except Exception as e:
            print(f"Error al rankear los datos: {e}")

    def rankear_menores(self, group_by_column, rank_column, method):
        try:
            self.df[f'{rank_column}'] = self.df.groupby(group_by_column)[rank_column].rank(method=method, ascending=True)
        except Exception as e:
            print(f"Error al rankear los datos: {e}")

    def pivotar_datos(self, index_column, columns_column, values_column):
        try:
            pivot_df = self.df.pivot(index=index_column, columns=columns_column, values=values_column).reset_index()
            return pivot_df
        except Exception as e:
            print(f"Error al pivotar los datos: {e}")
            return pd.DataFrame()

    def obtener_datos(self):
        return self.df
