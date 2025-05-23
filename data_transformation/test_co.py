import oracledb

conn = oracledb.connect(
    user="e2203009", 
    password="Yepkam46",
    dsn="ora23ai.univ-ubs.fr:1521/ORAETUD"
)

print("Connexion réussie !")
conn.close()