const { createClient } = require("@supabase/supabase-js")
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

module.exports = async function handler(req, res){
  // 🔥 STREAM REALTIME
  res.writeHead(200, {
    "Content-Type": "text/plain; charset=utf-8",
    "Transfer-Encoding": "chunked"
  })

  function log(msg){
    const time = new Date().toLocaleTimeString("pt-BR")
    const linha = `[${time}] ${msg}`
    console.log(linha)
    res.write(linha + "\n")
  }

  try{

    if(req.method !== "POST"){
      log("❌ Método inválido")
      res.end()
      return
    }

    const { empresa, dataInicio, dataFim } = req.body

    if(!empresa){
      log("❌ Empresa não enviada")
      res.end()
      return
    }

    const hoje = new Date().toISOString().slice(0,10)
    const inicio = dataInicio || hoje
    const fim = dataFim || hoje

    const startTotal = Date.now()

    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("🚀 INICIANDO IMPORTAÇÃO PROFISSIONAL")
    log(`🏢 Empresa: ${empresa}`)
    log(`📅 Período: ${inicio} → ${fim}`)

    // ================= LOGIN =================
    log("🔐 Fazendo login...")

    const loginResp = await fetch(`${req.headers.origin}/api/login`,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ empresa })
    })

    const loginData = await loginResp.json()
    const token = loginData.accessToken || loginData.token

    if(!token){
      log("❌ Token não retornado")
      res.end()
      return
    }

    log("✅ Token recebido")

    // ================= CONFIG =================
    const urls = {
      VAREJO_URL_MERCATTO: "https://mercatto.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_VILLA: "https://deliciagourmet.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_PADARIA: "https://mercattodelicia.varejofacil.com/api/v1/venda/cupons-fiscais",
      VAREJO_URL_DELICIA: "https://villachopp.varejofacil.com/api/v1/venda/cupons-fiscais"
    }

    const baseURL = urls[empresa]

    if(!baseURL){
      log("❌ Empresa inválida")
      res.end()
      return
    }

    // ================= VARIÁVEIS =================
    let pagina = 1
    const count = 200

    let totalCupons = 0
    let totalPagamentos = 0
    let totalPaginas = 0

    const ids = new Set()

    log("📡 INICIANDO PAGINAÇÃO...\n")

    // ================= LOOP =================
    while(true){

      const url = `${baseURL}?pagina=${pagina}&count=${count}&q=data=ge=${inicio};data=le=${fim}`

      const t0 = Date.now()

      let response

      // 🔁 RETRY INTELIGENTE
      for(let tentativa=1; tentativa<=3; tentativa++){
        try{
          response = await fetch(url,{
            headers:{
              Authorization: token,
              Accept:"application/json"
            }
          })
          if(response.ok) break
        }catch(e){}

        log(`⚠️ Tentativa ${tentativa} falhou...`)
        await new Promise(r => setTimeout(r, 500 * tentativa))
      }

      if(!response || !response.ok){
        log(`❌ ERRO API (página ${pagina}) - ignorando`)
        pagina++
        continue
      }

      const tempoReq = ((Date.now() - t0)/1000).toFixed(2)

      const json = await response.json()
      const items = json.items || []

      log(`📄 Página ${pagina} | Itens: ${items.length} | Tempo: ${tempoReq}s`)

      if(items.length === 0){
        log("🏁 Última página alcançada")
        break
      }

      const inserts = []
      const pagamentos = []

      for(const cupom of items){

        const unique_id = empresa + "_" + cupom.id

        if(ids.has(unique_id)){
          log(`⚠️ Cupom duplicado ignorado: ${cupom.id}`)
          continue
        }

        ids.add(unique_id)

        log(`🧾 Cupom ${cupom.id} | R$ ${cupom.valorTotal}`)

const valor_total = Number(cupom.valorTotal || 0)
const cancelado = !!cupom.cancelada

let valor_liquido = 0
let finalizadoraPrincipal = null

if(Array.isArray(cupom.finalizacoes) && cupom.finalizacoes.length > 0){

  valor_liquido = cupom.finalizacoes.reduce((total,f)=>{
    return total + (Number(f.valor || 0) - Number(f.troco || 0))
  },0)

  const maior = cupom.finalizacoes.reduce((a,b)=>
    (Number(a.valor||0) > Number(b.valor||0) ? a : b)
  )

  finalizadoraPrincipal = Number(maior.finalizadoraId || 0)
}

inserts.push({
  unique_id,
  empresa,
  empresa_id: empresa,
  venda_id: cupom.id,
  data: cupom.data,
  valor_total,
  valor_liquido,
  finalizadora_principal: finalizadoraPrincipal,
  cancelado,
  raw: cupom
})
      // ================= INSERT CUPONS =================
      if(inserts.length > 0){

        const tInsert = Date.now()

        const { error } = await supabase
          .from("cupons_importados")
          .upsert(inserts, { onConflict:"unique_id" })

        if(error){
          log("❌ ERRO INSERT CUPONS: " + error.message)
        }else{
          totalCupons += inserts.length
        }

        const tempoInsert = ((Date.now() - tInsert)/1000).toFixed(2)

        log(`💾 Inseridos: ${inserts.length} | Tempo DB: ${tempoInsert}s`)
      }

      // ================= INSERT PAGAMENTOS =================
      if(pagamentos.length > 0){

        await supabase
          .from("cupons_pagamentos")
          .insert(pagamentos)

        totalPagamentos += pagamentos.length

        log(`💳 Pagamentos inseridos: ${pagamentos.length}`)
      }

      totalPaginas++

      // 🔒 PROTEÇÃO LOOP
      if(pagina > 50){
        log("⛔ Limite de segurança atingido (50 páginas)")
        break
      }

      pagina++

      await new Promise(r => setTimeout(r, 120))
    }

    const tempoTotal = ((Date.now() - startTotal)/1000).toFixed(2)

    log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log("✅ FINALIZADO COM SUCESSO")
    log(`📊 Total cupons: ${totalCupons}`)
    log(`💳 Total pagamentos: ${totalPagamentos}`)
    log(`📄 Total páginas: ${totalPaginas}`)
    log(`⏱ Tempo total: ${tempoTotal}s`)
    log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    res.end()

  }catch(e){

    console.log("💥 ERRO GERAL:", e.message)
    res.write("💥 ERRO: " + e.message)
    res.end()
  }
}
