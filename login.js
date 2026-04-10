import { createClient } from "@supabase/supabase-js"

export default async function handler(req, res){

  console.log("🚀 START IMPORTAR CUPONS")

  try{

    // ================= VALIDA ENV =================
    if(!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE){
      return res.status(500).json({
        ok:false,
        error:"SUPABASE NÃO CONFIGURADO"
      })
    }

    const supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_SERVICE_ROLE
    )

    const { empresa, empresa_nome, dataInicio, dataFim } = req.body || {}

    console.log("📩 BODY:", req.body)

    if(!empresa){
      return res.status(400).json({
        ok:false,
        error:"EMPRESA NÃO INFORMADA"
      })
    }

    const hoje = new Date().toISOString().slice(0,10)
    const inicio = dataInicio || hoje
    const fim = dataFim || hoje

    console.log("📅 PERIODO:", inicio, "->", fim)

    // ================= LOGIN (API INTERNA) =================
    console.log("🔐 LOGIN...")

    const loginResp = await fetch("https://varejo-six.vercel.app/api/login",{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ empresa })
    })

    const loginText = await loginResp.text()

    console.log("📥 LOGIN RAW:", loginText.slice(0,200))

    let loginData
    try{
      loginData = JSON.parse(loginText)
    }catch{
      return res.status(500).json({
        ok:false,
        error:"LOGIN NÃO RETORNOU JSON",
        raw: loginText
      })
    }

    const token = loginData.accessToken || loginData.token

    if(!token){
      return res.status(500).json({
        ok:false,
        error:"TOKEN NÃO RETORNADO",
        loginData
      })
    }

    console.log("✅ TOKEN OK")

    // ================= RECEBIMENTOS =================
    console.log("📦 BUSCANDO CUPONS...")

    const resp = await fetch("https://varejo-six.vercel.app/api/recebimentos",{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({
        token,
        empresa,
        dataInicio: inicio,
        dataFim: fim
      })
    })

    const text = await resp.text()

    console.log("📥 RECEBIMENTOS RAW:", text.slice(0,300))

    let json
    try{
      json = JSON.parse(text)
    }catch{
      return res.status(500).json({
        ok:false,
        error:"RECEBIMENTOS NÃO RETORNOU JSON",
        raw: text
      })
    }

    const cupons = json.items || []

    console.log("📊 TOTAL CUPONS:", cupons.length)

    if(cupons.length === 0){
      return res.json({
        ok:true,
        inseridos:0
      })
    }

    // ================= PROCESSAMENTO =================
    const inserts = []
    const pagamentos = []

    for(const cupom of cupons){

      const venda_id = cupom.id || cupom.vendaId || cupom.codigo

      if(!venda_id){
        console.log("⚠️ CUPOM SEM ID IGNORADO")
        continue
      }

      const unique_id = empresa + "_" + venda_id

      const valor_total = Number(cupom.valorTotal || 0)

      inserts.push({
        unique_id,
        empresa: empresa_nome,
        empresa_id: empresa,
        venda_id,
        data: cupom.data,
        valor_total,
        valor_liquido: valor_total,
        finalizadora_principal: cupom.finalizacoes?.[0]?.descricao || null,
        cancelado: !!cupom.cancelada,
        raw: cupom
      })

      if(Array.isArray(cupom.finalizacoes)){
        cupom.finalizacoes.forEach(f=>{
          pagamentos.push({
            cupom_unique_id: unique_id,
            finalizadora_id: String(f.finalizadoraId),
            finalizadora_nome: f.descricao,
            valor: Number(f.valor || 0)
          })
        })
      }

    }

    console.log("📦 PARA INSERT:", inserts.length)

    // ================= UPSERT =================
    const { error: erroInsert } = const BATCH = 50

for(let i=0;i<inserts.length;i+=BATCH){

  const chunk = inserts.slice(i, i + BATCH)

  const { error } = await supabase
    .from("cupons_importados")
    .upsert(chunk, { onConflict:"unique_id" })

  if(error){
    console.log("❌ ERRO CUPONS:", error.message)
    throw error
  }

  await new Promise(r=>setTimeout(r,50))
}

    if(erroInsert){
      console.log("❌ ERRO SUPABASE:", erroInsert.message)

      return res.status(500).json({
        ok:false,
        error: erroInsert.message
      })
    }

    console.log("✅ CUPONS SALVOS")

    // ================= PAGAMENTOS =================
    if(pagamentos.length){

      console.log("💳 PAGAMENTOS:", pagamentos.length)

      const { error: erroPag } = const BATCH_PAG = 100

for(let i=0;i<pagamentos.length;i+=BATCH_PAG){

  const chunk = pagamentos.slice(i, i + BATCH_PAG)

  const { error } = await supabase
    .from("cupons_pagamentos")
    .insert(chunk)

  if(error){
    console.log("⚠️ ERRO PAG:", error.message)
  }

  await new Promise(r=>setTimeout(r,50))
}

      

      if(erroPag){
        console.log("⚠️ ERRO PAGAMENTOS:", erroPag.message)
      }

    }

    console.log("🔥 FINALIZADO")

    return res.json({
      ok:true,
      inseridos: inserts.length,
      pagamentos: pagamentos.length
    })

  }catch(e){

    console.log("💥 ERRO GERAL:", e)

    return res.status(500).json({
      ok:false,
      error: e.message,
      stack: e.stack
    })

  }

}
