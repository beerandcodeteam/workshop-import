import csv
from datetime import datetime

num_rows = 5000000  # 10 milhões de linhas

header = [
    "unit",
    "block",
    "buy_value",
    "outstanding_balance",
    "name",
    "phone",
    "email",
    "cpf",
    "rg",
    "rg_emitter",
    "rg_issue_date",
    "address",
    "number",
    "complement",
    "zipcode",
    "district",
    "city",
    "state",
    "nationality",
    "naturalness",
    "mother_name",
    "father_name",
    "birthdate",
    "marital_status",
    "c_name",
    "c_phone",
    "c_email",
    "c_cpf",
    "c_rg",
    "c_rg_emitter",
    "c_rg_issue_date",
    "created_at",
    "updated_at"
]

with open(f'customers-{num_rows}.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    # Escreve o cabeçalho com as novas colunas
    writer.writerow(header)

    for i in range(1, num_rows + 1):
        unit = i
        block = i
        buy_value = 480074 + i
        outstanding_balance = 415766
        name = f"Exemplo de Cliente {i}"
        phone = f"1690000000{i:02d}"
        email = f"testecliente{i}@teste.com"
        cpf = f"{i:011d}"  # CPF com 11 dígitos
        rg = cpf
        rg_emitter = "SSP"
        rg_issue_date = "10/10/2021"
        address = "Rua teste"
        number = i
        complement = ""
        zipcode = "01001010"
        district = "Bairro de Teste"
        city = "São Paulo"
        state = "SP"
        nationality = "Brasileiro"
        naturalness = "São Paulo"
        mother_name = f"Teste mae {i}"
        father_name = f"Teste Pai {i}"
        birthdate = "29/10/1994"
        marital_status = "Casado"
        c_name = f"Teste conjuge {i}"
        c_phone = phone
        c_email = f"testeconjuge{i}@teste.com"
        c_cpf = cpf
        c_rg = rg
        c_rg_emitter = rg_emitter
        c_rg_issue_date = "10/10/2025"
        # Gera o timestamp atual para created_at e updated_at
        created_at = datetime.now().isoformat(sep=' ', timespec='seconds')
        updated_at = created_at

        writer.writerow([
            unit, block, buy_value, outstanding_balance, name, phone, email, cpf,
            rg, rg_emitter, rg_issue_date, address, number, complement, zipcode,
            district, city, state, nationality, naturalness, mother_name, father_name,
            birthdate, marital_status, c_name, c_phone, c_email, c_cpf, c_rg,
            c_rg_emitter, c_rg_issue_date, created_at, updated_at
        ])
