# ATUALIZACAO_CALCULO

## Dependências
Recomendado: `pip install -r requirements.txt` a partir desta pasta (ou da raiz que contém o ficheiro).

## Variáveis `.env` (exemplo)
Crie um ficheiro `.env` na **raiz do projecto** (junto a `main.py`) com, por exemplo:

- `HOST` — host MySQL
- `DB` — base de dados
- `USER` / `PASS` — credenciais

## Caminhos (Linux)
- Ficheiros de texto: `txt_handler/txt_files/` (criada automaticamente).
- Planilhas processadas: `calculation_automation/OUTPUT/`.
- Cópias arquivadas por data: `calculation_automation/PLANS_ARCHIVED/<dd-mm-aaaa>/` (ou o caminho em `PLANS_OUTPUT_DIR`).

## Google Drive
A integração com a API do Google Drive está **desligada** por agora. O ficheiro `google_api/drive.py` contém um *stub*; a implementação completa deverá ser recuperada do Git quando for reactivar o upload.

## Nota
O cálculo automático via `xlwings` continua a assumir **Microsoft Excel** (cenário Windows típico). No Linux, avalie trocar para outro fluxo (ex.: preencher com `openpyxl` sem recalcular) ou manter o executor no Windows.
