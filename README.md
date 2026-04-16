# Painel de Entregas da Secretaria Nacional de Políticas de Desenvolvimento Regional e Territorial - SDR

Permite visualizar as **quantidades de equipamentos entregues**, **valores executados** para todos os itens, **População beneficiária**, **Quantidade de municípios beneficiários**, **quantidade de convênios** e **Km de pavimentação**, podendo ser desagregados por Região, UFs, municípios, áreas específicas e programas da SDR.

## Relatório Metodológico: Consolidação e Estimativa de Entregas da SDR (Siconv)

## 1. Introdução
Este relatório descreve o fluxo metodológico do script de processamento de dados desenvolvido para extrair, categorizar e agregar dados brutos do Siconv. O pipeline gera tabelas consolidadas com foco nas entregas da Secretaria de Desenvolvimento Regional (SDR), com destaque para a nova etapa de higienização de métricas físicas e estimativa avançada de área pavimentada (em m²), posteriormente convertida para Km por meio da multiplicação por 6 mil.

## 2. Ingestão e Processamento Inicial dos Dados
A ingestão utiliza uma consulta SQL otimizada com junções (JOINs) entre tabelas estruturais (`Proposta`, `Convênio`, `Pagamento`, `Itens_DL`, etc.) do Siconv.

**Filtros de Escopo Aplicados na Origem:**
* Restrição ao Órgão Superior `53000` (Ministério da Integração e do Desenvolvimento Regional).
* Exclusão de projetos básicos rejeitados e convênios não assinados, rescindidos ou anulados.
* Foco em UGs específicas da SDR (`530023`, `530020`, `530036`, `74019`).
* Seleção estrita de itens de liquidação com valores válidos documentados.

## 3. Tratamento de Variáveis e Limpeza
Os dados passaram por sanitização para viabilizar as regras de negócio:
* Desmembramento da coluna `ACAO_ORCAMENTARIA` para isolar o código do Programa.
* Remoção de caracteres de controle inválidos via Expressões Regulares (Regex).
* Normalização de textos textuais (conversão para minúsculas e remoção de acentos) no objeto da proposta, nome do item e descrição do item do documento de liquidação.
* Correção de tipagem e substituição de separadores decimais nas colunas financeiras.

## 4. Motor de Categorização (Regex e Regras de Negócio)
A classificação dos itens baseia-se em um algoritmo de Regex hierárquico, contendo termos de inclusão e exclusão (para evitar falsos positivos):

1. **Despesas Financeiras:** Isolamento de aditivos e rendimentos.
2. **Projetos e Capacitação:** Separação de engenharia consultiva de obras físicas.
3. **Obras e Infraestrutura:** Classificação detalhada (Pavimentação, Pontes, Barragens, Edificações, etc.).
4. **Máquinas Pesadas e Caminhões:** Subclassificação avançada de frotas (Linha amarela, basculantes, compactadores, pipas).
5. **Tratores e Implementos:** Diferenciação entre o maquinário trator e seus implementos secundários.
6. **Políticas Regionais (Rotas):** Mapeamento transversal no objeto da proposta para cadeias produtivas (Cacau, Mel, Cordeiro, Açaí, TIC, etc.).

*Nota de Correção:* Equipamentos com preenchimento inconsistente de quantidade no documento de liquidação (menor que 1 ou maior que 100) tiveram seu valor unitário forçado para `1` para evitar distorções de contagem.

## 5. Extração de Métricas Físicas Brutas (Obras em M²)
Para recuperar as dimensões físicas das obras (m²), construiu-se uma CTE (Common Table Expression) específica:
* O uso de funções de janela (`DENSE_RANK()`) garantiu a extração exclusiva da meta associada à **versão mais recente e validada** do Projeto Básico.
* Aplicou-se a condição de existência (`EXISTS`) para garantir que apenas convênios com pagamentos reais transitassem para a base de métricas.
* O valor da metragem extraída (`QUANTIDADE_M2`) foi isolado por convênio e filtrado apenas para obras medidas estritamente em "M2".

---

## 6. Identificação de Outliers e Higienização do Custo por M²
Uma vez que os dados do Siconv dependem de preenchimento humano, a metragem informada (`MAX_QUANTIDADE_M2`) apresentou inconsistências graves. Para mitigar isso, implementou-se uma regra de detecção de *outliers* baseada no custo paramétrico da obra.

1. **Cálculo do Custo Bruto:** O custo desembolsado por metro quadrado foi calculado pela razão entre o Valor Desembolsado do Convênio e a Metragem Máxima informada.
2. **Corte por Limites de Domínio:** O método estatístico padrão de Intervalo Interquartil (IQR) foi descartado por gerar limite inferior negativo. Em seu lugar, adotou-se um corte baseado na realidade de mercado da construção civil brasileira (CBUQ, Paver, Tratamentos a Frio).
3. **Limites Aplicados:** Valores de custo unitário abaixo de **R$ 10,00/m²** ou acima de **R$ 1.500,00/m²** foram classificados como anomalias.
4. **Tratamento:** Registros fora dessa banda de plausibilidade tiveram sua quantidade original de M² anulada (`NaN`), preparando o terreno para a imputação algorítmica.

---

## 7. Modelagem e Estimativa Avançada de Área Pavimentada
Para lidar com a granularidade dos pagamentos (diversas notas fiscais em anos diferentes para a mesma obra) e com os dados anulados no passo anterior, desenvolveu-se um modelo de estimativa de área executada.

### 7.1. Cálculo do Custo por M² Ponderado
Para os convênios com metragem válida, o valor real do M² foi calculado levando em conta o grau de conclusão financeira da obra e o peso daquele pagamento específico no todo:

$$
Custo_{m^2} = \frac{Valor\ Agregado}{\left( \frac{Valor\ Agregado}{Soma\ Valor\ Agregado} \right) \times \left( \frac{Valor\ Desembolsado\ Conv}{Valor\ Repasse\ Conv} \right) \times Max\ Quantidade\ M^2}
$$

A partir dessa fórmula, extraiu-se a **mediana do custo por metro quadrado para cada ano de pagamento** (`MEDIANA_CUSTO_M2`), criando um referencial de mercado ajustado à inflação de cada período.

### 7.2. Imputação e Estimativa Final (`M2_estimado`) por ano
A consolidação da área pavimentada por pagamento obedeceu a uma lógica condicional bipartida:

* **Cenário A (Dados Válidos):** Se o convênio possui metragem confiável, a área executada na nota fiscal foi calculada rateando a metragem total pela fração financeira do pagamento:
  $M^2\_Estimado = \left( \frac{Valor\ Agregado}{Soma\ Valor\ Agregado} \right) \times Quantidade\ M^2$
* **Cenário B (Dados Ausentes ou Outliers):** Se a metragem original foi reprovada nos limites de **R\$ 10 - R\$ 1.500**, a área foi matematicamente imputada dividindo o valor daquele pagamento específico pela mediana do custo do ano correspondente:

$M^{2} {Estimado} = \frac{Valor\ Agregado}{Mediana\ Custo\ M^{2}}$

---

## 8. Limitações
As classificações podem conter erros devido à falta de informação ou informação incerta ou ambígua no objeto da proposta, nome ou descrição dos itens no documento de liquidação, como Nota Fiscal. 
