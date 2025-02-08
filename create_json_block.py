from prefect.blocks.system import JSON

# Carregar o conteúdo do arquivo JSON
with open("tickers-list.json", "r") as file:
    tickers_list = file.read()

# Criar e salvar o JSON Block
json_block = JSON(value=tickers_list)
json_block.save(name="tickers-list", overwrite=True)

print("JSON Block 'tickers-list' criado com sucesso!")