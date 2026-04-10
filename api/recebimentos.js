// /api/recebimentos.js
export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Método não permitido" });
  }

  try {
    const { token, dataInicio, dataFim, empresa } = req.body;

    if (!token) {
      return res.status(400).json({ error: "Token de autenticação ausente" });
    }

    if (!empresa) {
      return res.status(400).json({ error: "Empresa não informada" });
    }

    const urls = {
      VAREJO_URL_MERCATTO: "https://mercatto.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_VILLA: "https://deliciagourmet.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_PADARIA: "https://mercattodelicia.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_DELICIA: "https://villachopp.varejofacil.com/api/v1/venda/cupons-fiscais"
    };

    const baseURL = urls[empresa];
    if (!baseURL) {
      return res.status(400).json({ error: `Empresa '${empresa}' não reconhecida.` });
    }

    // 🔧 Formata apenas as datas (sem hora)
    const inicioFormatado = dataInicio;
    const fimFormatado = dataFim;

    console.log("🔍 Consulta de cupons");
    console.log("Empresa:", empresa);
    console.log("Período:", inicioFormatado, "→", fimFormatado);

    const count = 500;
    let start = 0;
    let allItems = [];

    // 🔄 Paginação
    while (true) {
      const url = `${baseURL}?start=${start}&count=${count}&q=dataVenda=ge=${inicioFormatado};dataVenda=le=${fimFormatado}`;
      console.log(`📡 Buscando página: ${url}`);

      const response = await fetch(url, {
        headers: {
          Authorization: token,
          Accept: "application/json"
        }
      });

      if (!response.ok) {
        const erro = await response.text();
        console.error(`❌ Erro na API (${empresa}):`, erro);
        return res.status(response.status).json({ error: erro });
      }

      const json = await response.json();

      if (!json.items || json.items.length === 0) {
        console.log("📭 Nenhum resultado encontrado nesta página.");
        break;
      }

      allItems = allItems.concat(json.items);
      start += count;

      if (json.items.length < count) break;
      if (start > 5000) break; // segurança
    }

    console.log(`✅ Total de cupons retornados (${empresa}): ${allItems.length}`);

    res.status(200).json({
      start: 0,
      total: allItems.length,
      items: allItems
    });

  } catch (error) {
    console.error("❌ Erro no recebimentos.js:", error);
    res.status(500).json({
      error: "Falha ao consultar API de recebimentos",
      details: error.message
    });
  }
}
