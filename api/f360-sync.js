import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

export default async function handler(req, res){

  // 🔥 BLOQUEIA GET (OBRIGA POST)
  if(req.method !== "POST"){
    return res.status(405).json({ error:"Use POST" })
  }

  // 🔥 DESATIVA CACHE
  res.setHeader("Cache-Control", "no-store, max-age=0")

  try{

const hoje = new Date().toLocaleDateString("en-CA", {
  timeZone: "America/Bahia"
})



    
const inicio = hoje
const fim = hoje

    let pagina = 1
    let totalInseridos = 0

    while(true){

      console.log("📄 Página:", pagina)

      const response = await fetch(
        `https://financas.f360.com.br/ParcelasDeTituloPublicAPI/ListarParcelasDeTitulos?pagina=${pagina}&tipo=Despesa&inicio=${inicio}&fim=${fim}&tipoDatas=Vencimento`,
        {
          method:"GET",
          headers:{
            "Authorization": `Bearer ${process.env.F360_TOKEN}`,
            "Content-Type":"application/json"
          }
        }
      )

const text = await response.text()

let json

try{
  json = JSON.parse(text)
}catch(e){

  console.log("❌ ERRO F360:", text)

  return res.status(500).json({
    error:"F360 retornou inválido",
    raw:text
  })
}

      
      if(!json?.Result?.Parcelas) break

      const parcelas = json.Result.Parcelas

      if(parcelas.length === 0) break

      // 🔥 PROCESSA E SALVA
const rows = parcelas
  .filter(p => !p.Cancelada)
  .map(p => ({


    
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

const { data, error } = await supabase
  .from("f360_parcelas")
  .upsert(rows, { onConflict: "parcela_id" })
  .select()

if(error){
  console.log("❌ ERRO SUPABASE:", error)
}else{
  console.log("✅ UPSERT:", data.length)
  totalInseridos += data.length
}

      // 🔥 PARA SE ACABOU
const totalPaginas = Number(json?.Result?.QuantidadeDePaginas || 1)

if(pagina >= totalPaginas) break

      
      pagina++
    }

    return res.json({
      ok:true,
      totalInseridos
    })

  }catch(e){

    return res.status(500).json({
      error:"Erro no sync F360",
      details:e.message
    })

  }

}
