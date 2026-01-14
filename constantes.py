import os
from datetime import datetime

# Define o ano letivo atual dinamicamente
ANO_LETIVO_ATUAL = datetime.now().year
# Se o script rodar no final de 2025 já preparando 2026, você pode ajustar a lógica
# ANO_LETIVO_ATUAL = 2026 

# Nome da tabela de origem centralizado
TABELA_ALUNOS_GERAL = f"alunos_{str(ANO_LETIVO_ATUAL)[2:]}_geral" 
# Resultado: "alunos_26_geral" em 2026

# Token de autenticação para API
API_TOKEN = "Token 443864674b4a856e86990a6c8b3241d3a08e7d8e"

# Cabeçalhos padrão para requisições HTTP
HEADERS = {
    "Authorization": f"{API_TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Configuração do banco de dados
DB_CONFIG = {
    "database": "BOLETOS",
    "user": "postgres",
    "password": "teste",
    "host": "192.168.1.163",
    "port": "5432"
}

# Mapeamento de código -> nome da unidade
CODIGO_PARA_UNIDADE = {
    "01": "Bento Ribeiro",
    "02": "Madureira",
    "03": "Santa Cruz",
    "04": "Cascadura",
    "05": "Taquara",
    "06": "Nilópolis",
    "09": "Seropédica",
    "10": "Barra da Tijuca",
    "11": "Campo Grande",
    "13": "Mangueira",
    "14": "Maricá",
    "15": "Ilha do Governador",
    "16": "Freguesia",
    "17": "Recreio dos Bandeirantes"
}

# IDs das coordenações por unidade
COORDINATION_IDS = {
    "Barra da Tijuca": {
        "Anos Iniciais": "0f04befc-aedd-4020-a85d-ae295b677412",
        "Anos Finais": "79bebe52-d907-4313-b83d-9818c57f59a6",
        "Ensino Médio": "086fcd65-1c90-42c5-b8f9-66b033f6c603"
    },
    "Bento Ribeiro": {
        "Anos Iniciais": "f286f842-b8a7-4834-818e-eedea7d5f7c2",
        "Anos Finais": "972b0161-d403-4b50-ab79-dd3ce20f169a",
        "Ensino Médio": "467e5fc6-5c4d-46d2-aa1a-7d049743852f"
    },
    "Campo Grande": {
        "Anos Iniciais": "b85ec9ad-aef4-4c08-9b43-e6e18be31511",
        "Anos Finais": "98c2d145-d0bd-41cd-934a-3e7d5efa0eef",
        "Ensino Médio": "4aff0b60-294a-469f-87e4-b47803621585"
    },
    "Cascadura": {
        "Anos Iniciais": "31861f2d-8bb1-4624-a121-5146c1a8405f",
        "Anos Finais": "4cf321dd-ef18-4cbd-ab1d-e94dd54a20dd",
        "Ensino Médio": "746f35a2-27fc-4e9b-b3d5-4aad7799dca7"
    },
    "Freguesia": {
        "Anos Iniciais": "95f002ff-037f-413a-8e48-199b3f92f75f",
        "Anos Finais": "3748054e-3c5d-4257-90b2-84d281c8d5ca",
        "Ensino Médio": "1ea65818-3cf3-42be-a59c-cc0aa28d8bfc"
    },
    "Ilha do Governador": {
        "Anos Iniciais": "b33b93a7-0b2c-4b14-84d1-3e49f46bc0c5",
        "Anos Finais": "a5093693-9d59-4a94-a497-01050c907578",
        "Ensino Médio": "37f7ef66-127c-4cbf-9fca-88090538751a"
    },
    "Madureira": {
        "Anos Iniciais": "79b383e9-0adb-4a55-aac6-4590836d22f9",
        "Anos Finais": "480111d4-65be-4dd1-8065-793a303e9cb0",
        "Ensino Médio": "5b8c71a0-c4c9-4f71-8ff4-c58745f67738"
    },
    "Mangueira": {
        "Anos Iniciais": "b906493d-2fb1-42bf-a8d1-8846ba533f2c",
        "Anos Finais": "3274f5e2-f878-420a-8ac8-07c2a8a9c9a0",
        "Ensino Médio": "4f576314-3210-4ba4-9e6d-45d6239964bf"
    },
    "Maricá": {
        "Anos Iniciais": "9caeb8b4-bf53-4736-8f73-68c99b54b827",
        "Anos Finais": "e725625f-20d0-4408-aaad-5622b34cf0ff",
        "Ensino Médio": "898c55e2-3422-43d5-acfc-5ff8d8bdea6c"
    },
    "Nilópolis": {
        "Anos Iniciais": "0aba4d71-6cfd-4cf0-ba13-720a94f1d183",
        "Anos Finais": "61bc5631-dbdc-46a4-8b65-1ae272c1b47e",
        "Ensino Médio": "350baf94-6add-4531-8e11-51f56c569524"
    },
    "Recreio dos Bandeirantes": {
        "Anos Iniciais": "f6edfb0a-3126-4f78-9d32-2d6aca9f602f",
        "Anos Finais": "c85649ca-772d-4ff4-8e80-5e5cba39ed89",
        "Ensino Médio": "4aad3b57-9a83-49e5-877d-b2638734b595"
    },
    "Santa Cruz": {
        "Anos Iniciais": "d843fb10-0bde-4163-8297-ed51db858e35",
        "Anos Finais": "2c79be68-3119-4f8b-8255-77d3c1ab4b3c",
        "Ensino Médio": "97521433-8fc5-454a-9215-af10d05069ff"
    },
    "Seropédica": {
        "Anos Iniciais": "942c4bbb-9c07-4bf6-bfa8-092026acbb31",
        "Anos Finais": "2d76bc0a-5ecb-4165-a6c2-ad2e16c347b3",
        "Ensino Médio": "051ab556-9bc2-4dc6-a109-e8763f4121d8"
    },
    "Taquara": {
        "Anos Iniciais": "6f5b8d57-7b73-446c-b716-dfb371afbbc9",
        "Anos Finais": "959ab757-89be-46ce-bd3f-e84c8c7c5724",
        "Ensino Médio": "a86f9e35-48b2-40cc-85ba-62a66aab345b"
    }
}

