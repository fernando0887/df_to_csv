import pandas as pd
import psycopg2


try:
    connect = psycopg2.connect(host ='localhost', database ='nwd', user='postgres', password='88227874')
    print('Conexão estabelecida')

except:
    print('Não foi possível conectar.')


#ínicia a transação com o banco de dados
connect.cursor()
#a varíavel carrega as consultas que serão feitas com as colunas específicas
tbl_1 = 'select customer_id, company_name, city, country from customers'
#método que transforma as consultas em um dataframe
df_1 = pd.read_sql(tbl_1, connect)
print(df_1.head(20))
#salva o dataframe em um arquivo csv
df_1.to_csv('C:/Users/nando/Documents/customer.csv')

connect.cursor()
tbl_2= 'select product_id,product_name,category_id from products'
df_2 = pd.read_sql(tbl_2,connect)
print(df_2.head(20))
df_2.to_csv('C:/Users/nando/Documents/product.csv')

connect.cursor()
tbl_3 = 'select supplier_id, company_name, country from suppliers'
df_3 = pd.read_sql(tbl_3,connect)
print(df_3.head(20))
df_3.to_csv('C:/Users/nando/Documents/supplier.csv')

connect.cursor()
tbl_4 = 'select employee_id, last_name, reports_to from employees'
df_4 = pd.read_sql(tbl_4,connect)
print(df_4.head(20))
df_4.to_csv('C:/Users/nando/Documents/employee.csv')

#método para concatenar todos os dataframes em um arquivo csv
dft = pd.concat([df_1,df_2,df_3,df_4], ignore_index= True)
dft.to_csv('C:/Users/nando/Documents/order_facts.csv')