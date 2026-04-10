import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

export default async function handler(req, res){

  if(req.method !== "POST"){
    return res.status(405).json({ error:"Use POST" })
  }

  res.setHeader("Cache-Control", "no-store, max-age=0")

  try{

    const hoje = new Date().toLocaleDateString("en-CA", {
      timeZone: "America/Bahia"
    })

    const inicio = hoje
    const fim = hoje

    console.log("📅 PERÍODO:", inicio, "→", fim)

    let pagina = 1
    let totalInseridos = 0

    while(true){

      console.log("━━━━━━━━━━━━━━━━━━━━━━")
      console.log("📄 Página:", pagina)

      const url = `https://financas.f360.com.br/ParcelasDeTituloPublicAPI/ListarParcelasDeTitulos?pagina=${pagina}&tipo=Despesa&inicio=${inicio}&fim=${fim}&tipoDatas=Vencimento`

      console.log("🌐 URL:", url)

      const response = await fetch(url,{
        method:"GET",
        headers:{
          "Authorization": `Bearer ${process.env.F360_TOKEN}`,
          "Content-Type":"application/json"
        }
      })

      console.log("📡 STATUS:", response.status)

      const text = await response.text()

      console.log("📦 RAW:", text.slice(0,500)) // mostra só início

      let json

      try{
        json = JSON.parse(text)
      }catch(e){
        console.log("❌ JSON INVÁLIDO")
        return res.status(500).json({ error:"JSON inválido", raw:text })
      }

      const parcelas = json?.Result?.Parcelas || []

      console.log("📊 TOTAL PARCELAS:", parcelas.length)

      if(parcelas.length === 0){
        console.log("⚠️ SEM DADOS NESSA PÁGINA")
        break
      }

      // 🔥 SALVA TUDO (SEM FILTRO)
      const rows = parcelas.map(p => ({

        parcela_id: p.ParcelaId,

        tipo: p.Tipo,
        numero: p.Numero,

        vencimento: p.Vencimento,
        liquidacao: p.Liquidacao,

        valor: p.ValorBruto,

        empresa: p?.DadosDoTitulo?.Empresa?.Nome || "",
        fornecedor: p?.DadosDoTitulo?.ClienteFornecedor?.Nome || "",

        categoria: p?.Rateio?.[0]?.PlanoDeContas || "",
        centro_custo: p?.Rateio?.[0]?.CentroDeCusto || "",

        conta: p.Conta,
        meio_pagamento: p.MeioDePagamento,
        status: p.Status,

        raw: p,
        atualizado_em: new Date()
      }))

      console.log("💾 SALVANDO:", rows.length)

      const { data, error } = await supabase
        .from("f360_parcelas")
        .upsert(rows, { onConflict: "parcela_id" })
        .select()

      if(error){
        console.log("❌ ERRO SUPABASE:", error)
      }else{
        console.log("✅ INSERIDOS:", data.length)
        totalInseridos += data.length
      }

      const totalPaginas = Number(json?.Result?.QuantidadeDePaginas || 1)

      console.log("📄 TOTAL PÁGINAS:", totalPaginas)

      if(pagina >= totalPaginas){
        console.log("🏁 FINALIZADO")
        break
      }

      pagina++
    }

    return res.json({
      ok:true,
      totalInseridos
    })

  }catch(e){

    console.log("🔥 ERRO GERAL:", e)

    return res.status(500).json({
      error:"Erro no sync F360",
      details:e.message
    })
  }
}
