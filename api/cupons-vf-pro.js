import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

// 🔥 MAPA VAREJO FÁCIL
const URLS = {
  VAREJO_URL_MERCATTO: "https://mercatto.varejofacil.com/api/v1/venda/cupons-fiscais",
  VAREJO_URL_VILLA: "https://deliciagourmet.varejofacil.com/api/v1/venda/cupons-fiscais",
  VAREJO_URL_PADARIA: "https://mercattodelicia.varejofacil.com/api/v1/venda/cupons-fiscais",
  VAREJO_URL_DELICIA: "https://villachopp.varejofacil.com/api/v1/venda/cupons-fiscais"
}

export default async function handler(req, res){

  const body = req.body || {}

  // 🔥 MODO PAINEL (JSON LIMPO)
  if(body.modo === "BUSCAR"){
    return buscarModoJSON(req, res)
  }

  if(body.modo === "INSERIR"){
    return inserirModoJSON(req, res)
  }

  // 🔥 MODO STREAM (ZAFIA)
  return modoStream(req, res)
}

//////////////////////////////////////////////////////
// 🔍 MODO BUSCAR (PAINEL)
//////////////////////////////////////////////////////
async function buscarModoJSON(req, res){

  try{

    const { empresa, dataInicio, dataFim } = req.body

    const loginResp = await fetch(`${req.headers.origin}/api/login`,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ empresa })
    })

    const loginData = await loginResp.json()
    const token = loginData.accessToken || loginData.token

    if(!token){
      return res.status(400).json({ error:"Token inválido" })
    }

    const baseURL = URLS[empresa]
    if(!baseURL){
      return res.status(400).json({ error:"Empresa inválida" })
    }

    let pagina = 1
    let todos = []
    let ids = new Set()

    while(true){

      const url = `${baseURL}?pagina=${pagina}&count=1000&q=data=ge=${dataInicio};data=le=${dataFim}`

      const response = await fetch(url,{
        headers:{
          Authorization: token,
          Accept:"application/json"
        }
      })

      if(!response.ok){
        break
      }

      const json = await response.json()
      const items = json.items || []

      if(items.length === 0){
        break
      }

      for(const cupom of items){

        const id = empresa + "_" + cupom.id

        if(ids.has(id)) continue
        ids.add(id)

        todos.push({
          id,
          valor: Number(cupom.valor || 0),
          data: cupom.data,
          empresa,
          raw: cupom
        })
      }

      pagina++
      if(pagina > 200) break
    }

    return res.json({
      total: todos.length,
      cupons: todos
    })

  }catch(e){
    return res.status(500).json({ error:e.message })
  }
}

//////////////////////////////////////////////////////
// 💾 MODO INSERIR (LOTE)
//////////////////////////////////////////////////////
async function inserirModoJSON(req, res){

  try{

    const cupons = req.body.cupons || []

    if(!cupons.length){
      return res.json({ inseridos:0 })
    }

    const ids = cupons.map(c => c.id)

    const { data: existentes } = await supabase
      .from("cupons_importados")
      .select("unique_id")
      .in("unique_id", ids)

    const set = new Set(existentes?.map(e => e.unique_id) || [])

    const novos = cupons.filter(c => !set.has(c.id))

    const payload = novos.map(c => ({
      unique_id: c.id,
      empresa: c.empresa,
      empresa_id: c.empresa,
      venda_id: c.raw?.id,
      data: c.data,
      cancelado: !!c.raw?.cancelada,
      raw: c.raw
    }))

    if(payload.length > 0){

      await supabase
        .from("cupons_importados")
        .upsert(payload, { onConflict:"unique_id" })
    }

    return res.json({
      inseridos: payload.length,
      ignorados: cupons.length - payload.length
    })

  }catch(e){
    return res.status(500).json({ error:e.message })
  }
}

//////////////////////////////////////////////////////
// 🚀 MODO STREAM (SEU ORIGINAL MELHORADO)
//////////////////////////////////////////////////////
async function modoStream(req, res){

  res.writeHead(200,{
    "Content-Type":"text/plain",
    "Transfer-Encoding":"chunked"
  })

  function log(msg){
    res.write(`[${new Date().toLocaleTimeString()}] ${msg}\n`)
  }

  try{

    const { empresa, dataInicio, dataFim } = req.body

    log("🚀 INICIANDO")

    const loginResp = await fetch(`${req.headers.origin}/api/login`,{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({ empresa })
    })

    const loginData = await loginResp.json()
    const token = loginData.accessToken || loginData.token

    if(!token){
      log("❌ Token inválido")
      res.end()
      return
    }

    const baseURL = URLS[empresa]

    let pagina = 1
    let total = 0

    while(true){

      const url = `${baseURL}?pagina=${pagina}&count=1000&q=data=ge=${dataInicio};data=le=${dataFim}`

      const response = await fetch(url,{
        headers:{ Authorization: token }
      })

      if(!response.ok){
        log("❌ ERRO API")
        break
      }

      const json = await response.json()
      const items = json.items || []

      if(items.length === 0){
        log("🏁 FIM")
        break
      }

      log(`📄 Página ${pagina} | ${items.length} cupons`)

      total += items.length
      pagina++

      if(pagina > 200) break
    }

    log(`📊 TOTAL: ${total}`)

    res.end()

  }catch(e){
    res.write("💥 " + e.message)
    res.end()
  }
}
