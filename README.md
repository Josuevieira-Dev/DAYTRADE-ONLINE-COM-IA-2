# IA Crypto Trader M1 — versão 1.0

Painel responsivo para acompanhar BTCUSDT, ETHUSDT, SOLUSDT e XRPUSDT no mercado Binance USDⓈ-M Futures, com candles de 1 minuto, indicadores técnicos e sinais LONG/SHORT/AGUARDAR.

## O que esta versão faz
- Obtém histórico público de candles 1m via REST da Binance Futures.
- Recebe atualizações de candles em tempo real pelo WebSocket público.
- Calcula EMA 9/20/50, RSI 14, ATR e pontuação técnica simples.
- Exibe gráfico, sinais, stop e alvo estimados.
- Permite configurar capital de referência e risco entre 0,1% e 2%.
- Interface adaptável a computador e celular.

## Limitações importantes
- Esta versão é **somente monitoramento e simulação**. Não envia ordens e não acessa saldo da conta.
- A quantidade calculada é uma estimativa; antes de qualquer execução é necessário aplicar filtros de lote/tick, margem, alavancagem, taxas, slippage e regras de risco da corretora.
- Stop e alvo são níveis indicativos, não ordens protetivas efetivamente colocadas.
- O score é uma regra heurística, não uma probabilidade estatística de lucro.
- A análise usa candles fechados para formar sinais; o candle em formação aparece no gráfico.
- A implementação do MACD completo, backtest, persistência e validação de estratégia deve ser feita antes de habilitar automação.

## Instalação (Windows)
1. Instale Python 3.11 ou superior.
2. Extraia o ZIP.
3. Abra o Prompt de Comando dentro da pasta.
4. Execute:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn app:app --host 0.0.0.0 --port 8000
```

5. Abra `http://localhost:8000` no computador.
6. Para acessar pelo celular na mesma rede Wi-Fi, use `http://IP-DO-COMPUTADOR:8000`.

## Segurança
Não coloque chaves API nesta versão. Nunca compartilhe Secret Key. Quando houver módulo de execução, começar exclusivamente no Futures Testnet, com chave sem permissão de saque, limite de exposição e botão de emergência.

## Próximas etapas
1. Backtest com taxas, funding e slippage.
2. Paper trading persistente e relatório de desempenho.
3. Execução de ordens no ambiente demo/testnet, com confirmação e ordens protetivas.
4. Somente depois de validação, avaliar integração live com limites rigorosos.
