import { createClient } from "@supabase/supabase-js"

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
)

// 🔐 LOGIN VAREJO FÁCIL
async function getToken(){

  const response = await fetch(
    "https://financas.f360.com.br/PublicLoginAPI/DoLogin",
    {
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify({
        Token: process.env.F360_LOGIN_TOKEN
      })
    }
  )

  const json = await response.json()

  if(!json?.Token){
    throw new Error("Token não retornado")
  }

  return json.Token
}

// 🔍 BUSCAR CUPONS
async function buscarCupons(token, body){

  const response = await fetch(
    "https://financas.f360.com.br/FluxoCaixaAPI/GetFluxoCaixaDetalhado",
    {
      method:"POST",
      headers:{
        "Content-Type":"application/json",
        "Authorization": token
      },
      body: JSON.stringify({
        DataInicio: body.dataInicio,
        DataFim: body.dataFim,
        Empresa: body.empresa
      })
    }
  )

  const json = await response.json()

  if(!json?.Dados){
    return []
  }

  // 🔥 NORMALIZAÇÃO (PADRÃO VAREJO FÁCIL)
  const cupons = json.Dados.map(item => ({

    id: String(item.NumeroCupom || item.Id),

    valor: Number(item.ValorLiquido || item.Valor || 0),

    data: item.Data?.split("T")[0],

    hora: item.Data?.split("T")[1]?.slice(0,8) || "00:00:00",

    empresa: body.empresa,

    finalizadora: item.FormaPagamento || "OUTROS",

    cancelado: item.Cancelado || false

  }))

  return cupons
}

// 💾 INSERIR CUPONS
async function inserirCupons(cupons){

  if(!cupons.length) return { inseridos:0 }

  const ids = cupons.map(c => c.id)

  // 🔥 EVITA DUPLICADOS
  const { data: existentes } = await supabase
    .from("resumo_vendas")
    .select("unique_id")
    .in("unique_id", ids)

  const existentesSet = new Set(
    existentes?.map(e => e.unique_id) || []
  )

  const novos = cupons.filter(c => !existentesSet.has(c.id))

  if(!novos.length){
    return { inseridos:0 }
  }

  // 🔥 INSERT EM LOTE
  const payload = novos.map(c => ({
    unique_id: c.id,
    valor_liquido: c.valor,
    data: c.data,
    hora: c.hora,
    empresa_id: c.empresa,
    finalizadora: c.finalizadora,
    cancelado: c.cancelado
  }))

  const { error } = await supabase
    .from("resumo_vendas")
    .insert(payload)

  if(error){
    throw new Error(error.message)
  }

  return { inseridos: payload.length }
}

// 🚀 HANDLER
export default async function handler(req, res){

  try{

    if(req.method !== "POST"){
      return res.status(405).json({ error:"Use POST" })
    }

    const body = req.body

    if(!body.modo){
      return res.status(400).json({ error:"Modo não informado" })
    }

    // 🔥 LOGIN
    const token = await getToken()

    // =========================
    // 🔍 BUSCAR
    // =========================
    if(body.modo === "BUSCAR"){

      const cupons = await buscarCupons(token, body)

      return res.status(200).json({
        total: cupons.length,
        cupons
      })
    }

    // =========================
    // 💾 INSERIR
    // =========================
    if(body.modo === "INSERIR"){

      const resultado = await inserirCupons(body.cupons || [])

      return res.status(200).json({
        ok:true,
        ...resultado
      })
    }

    return res.status(400).json({ error:"Modo inválido" })

  }catch(e){

    console.error("❌ ERRO API:", e.message)

    return res.status(500).json({
      error:e.message
    })
  }
}
