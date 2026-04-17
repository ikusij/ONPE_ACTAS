import streamlit as st
import json
import altair as alt
import pandas as pd
from collections import defaultdict

def participantes_chart(participantes):
    rows = []
    for p in sorted(participantes, key=lambda x: x["votos"], reverse=True):
        rows.append({"nombre": p["nombre"], "votos": p["votos"], "tipo": "Votos actuales"})
        rows.append({"nombre": p["nombre"], "votos": p["votosAdicionales"], "tipo": "Votos adicionales"})
    df = pd.DataFrame(rows)
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("votos:Q", title="Votos"),
        y=alt.Y("nombre:N", sort="-x", title=None),
        color=alt.Color("tipo:N", scale=alt.Scale(range=["#4C78A8", "#F58518"])),
        order=alt.Order("tipo:N"),
        tooltip=["nombre", "tipo", alt.Tooltip("votos:Q", format=",")],
    ).properties(height=max(120, len(participantes) * 60))
    return chart

def get_votos_por_acta(data):
    p0 = next((p for p in data["participantesOutput"] if p["porcentajeVotos"] > 0), None)
    return (p0["votosAdicionales"] * 100 / (data["enviadasJee"] * p0["porcentajeVotos"])) if p0 and data["enviadasJee"] > 0 else 0

def adjusted_participantes(data, actas_eliminadas):
    vpa = get_votos_por_acta(data)
    return [
        {
            "nombre": p["nombre"],
            "votos": p["votos"],
            "votosAdicionales": int(vpa * (data["enviadasJee"] - actas_eliminadas) * p["porcentajeVotos"] / 100),
        }
        for p in data["participantesOutput"]
    ]

def render_aggregate(label, districts_data, actas_eliminadas_map):
    votos = defaultdict(lambda: {"votos": 0, "votosAdicionales": 0})
    total_eliminadas = sum(actas_eliminadas_map.get(u, 0) for u in districts_data)
    for ubigeo, data in districts_data.items():
        for p in adjusted_participantes(data, actas_eliminadas_map.get(ubigeo, 0)):
            votos[p["nombre"]]["votos"] += p["votos"]
            votos[p["nombre"]]["votosAdicionales"] += p["votosAdicionales"]

    projected = sorted(votos.items(), key=lambda x: x[1]["votos"] + x[1]["votosAdicionales"], reverse=True)

    with st.container(border=True):
        st.caption(label)
        gc1, gc2, gc3, gc4, gc5 = st.columns(5)
        gc1.metric("Total Actas", f"{sum(d['totalActas'] for d in districts_data.values()):,}")
        gc2.metric("Contabilizadas", f"{sum(d['contabilizadas'] for d in districts_data.values()):,}")
        gc3.metric("Enviadas JEE", f"{sum(d['enviadasJee'] for d in districts_data.values()):,}")
        gc4.metric("Pendientes", f"{sum(d['pendientesJee'] for d in districts_data.values()):,}")
        gc5.metric("Actas eliminadas", f"{total_eliminadas:,}")
        if len(projected) >= 2:
            l_name, l_stats = projected[0]
            s_name, s_stats = projected[1]
            l_total = l_stats["votos"] + l_stats["votosAdicionales"]
            s_total = s_stats["votos"] + s_stats["votosAdicionales"]
            margin = l_total - s_total
            st.info(
                f"**Líder proyectado: {l_name}** ({l_stats['votos']:,} actuales → {l_total:,} proyectados) "
                f"— aventaja a **{s_name}** ({s_stats['votos']:,} actuales → {s_total:,} proyectados) "
                f"por **{margin:,} votos**"
            )
        SANCHEZ = "ROBERTO HELBERT SANCHEZ PALOMINO"
        ALIAGA = "RAFAEL BERNARDO LÓPEZ ALIAGA CAZORLA"
        sanchez_actas = aliaga_actas = 0
        for ubigeo, data in districts_data.items():
            adj = adjusted_participantes(data, actas_eliminadas_map.get(ubigeo, 0))
            totals = {p["nombre"]: p["votos"] + p["votosAdicionales"] for p in adj}
            s, a = totals.get(SANCHEZ, 0), totals.get(ALIAGA, 0)
            if s > a:
                sanchez_actas += data["enviadasJee"]
            elif a > s:
                aliaga_actas += data["enviadasJee"]
        if sanchez_actas or aliaga_actas:
            sc1, sc2 = st.columns(2)
            sc1.metric("Actas JEE donde lidera Sánchez", f"{sanchez_actas:,}")
            sc2.metric("Actas JEE donde lidera Aliaga", f"{aliaga_actas:,}")

        agg_p = [{"nombre": n, "votos": s["votos"], "votosAdicionales": s["votosAdicionales"]} for n, s in votos.items()]
        st.altair_chart(participantes_chart(agg_p), use_container_width=True)

with open("results_additional_votes.json") as f:
    results = json.load(f)

with open("hierarchy.json") as f:
    hierarchy = json.load(f)

dept_names = [d["nombre"] for d in hierarchy]
dept_to_provinces = {d["nombre"]: [p["nombre"] for p in d["provincias"]] for d in hierarchy}
prov_to_districts = {p["nombre"]: [dist["nombre"] for dist in p["distritos"]] for d in hierarchy for p in d["provincias"]}
prov_to_ubigeos = {p["nombre"]: {dist["nombre"]: dist["ubigeo"] for dist in p["distritos"]} for d in hierarchy for p in d["provincias"]}

st.set_page_config(layout="wide")
st.title("Resultados Electorales - Votos Adicionales")

st.sidebar.header("Filtros")
scope = st.sidebar.selectbox("Ámbito", ["TODOS", "PERU", "EXTRANJERO"])
selected_dept = selected_prov = selected_dist = None

if scope in ("TODOS", "PERU"):
    selected_dept = st.sidebar.selectbox("Departamento", ["Todos"] + dept_names)
    if selected_dept and selected_dept != "Todos":
        selected_prov = st.sidebar.selectbox("Provincia", ["Todas"] + dept_to_provinces[selected_dept])
        if selected_prov and selected_prov != "Todas":
            selected_dist = st.sidebar.selectbox("Distrito", ["Todos"] + prov_to_districts[selected_prov])

def ubigeo_matches(ubigeo, data):
    if scope == "EXTRANJERO":
        return int(ubigeo) >= 900000
    if scope == "PERU" and int(ubigeo) >= 900000:
        return False
    if selected_dept and selected_dept != "Todos" and data["departamento"] != selected_dept:
        return False
    if selected_prov and selected_prov != "Todas" and data["provincia"] != selected_prov:
        return False
    if selected_dist and selected_dist != "Todos":
        return ubigeo == prov_to_ubigeos.get(selected_prov, {}).get(selected_dist)
    return True

filtered = {u: d for u, d in results.items() if ubigeo_matches(u, d)}
st.caption(f"Mostrando {len(filtered)} de {len(results)} distritos")

# Read stepper values from session_state (available after first render)
actas_eliminadas_map = {u: st.session_state.get(u, 0) for u in filtered}

if filtered:
    # Top-level aggregate
    render_aggregate("Agregado global", filtered, actas_eliminadas_map)

    # Grouping
    if selected_prov and selected_prov != "Todas":
        group_key = "provincia"
    elif selected_dept and selected_dept != "Todos":
        group_key = "provincia"
    else:
        group_key = "departamento"

    groups = defaultdict(dict)
    for ubigeo, data in filtered.items():
        groups[data[group_key]][ubigeo] = data

    GRID_COLS = 2

    use_toggle = not (selected_prov and selected_prov != "Todas")

    for group_name, districts in sorted(groups.items()):
        st.markdown(f"### {group_name}")

        if use_toggle:
            render_aggregate(f"Agregado de {group_name}", districts, actas_eliminadas_map)

        items = list(districts.items())

        if use_toggle:
            toggle_key = f"show_{group_name}"
            if toggle_key not in st.session_state:
                st.session_state[toggle_key] = False
            label = f"▲ Ocultar {len(items)} distritos" if st.session_state[toggle_key] else f"▼ Ver {len(items)} distritos de {group_name}"
            if st.button(label, key=f"btn_{group_name}"):
                st.session_state[toggle_key] = not st.session_state[toggle_key]
                st.rerun()

        if not use_toggle or st.session_state[toggle_key]:
            for i in range(0, len(items), GRID_COLS):
                cols = st.columns(GRID_COLS)
                for j, (ubigeo, data) in enumerate(items[i:i + GRID_COLS]):
                    with cols[j].container(border=True):
                        st.subheader(data["distrito"])
                        st.caption(ubigeo)

                        c1, c2 = st.columns(2)
                        c1.metric("Total Actas", data["totalActas"])
                        c2.metric("Contabilizadas", f"{data['contabilizadas']} ({data['actasContabilizadas']}%)")

                        c3, c4 = st.columns(2)
                        c3.metric("Enviadas JEE", f"{data['enviadasJee']} ({data['actasEnviadasJee']}%)")
                        c4.metric("Pendientes", f"{data['pendientesJee']} ({data['actasPendientesJee']}%)")

                        max_actas = int(data["enviadasJee"])
                        if max_actas > 0:
                            st.number_input("Actas a eliminar", min_value=0, max_value=max_actas, value=0, step=1, key=ubigeo)

                        actas_el = actas_eliminadas_map.get(ubigeo, 0)
                        adj = adjusted_participantes(data, actas_el)
                        dist_projected = sorted(adj, key=lambda x: x["votos"] + x["votosAdicionales"], reverse=True)
                        if len(dist_projected) >= 2:
                            l, s = dist_projected[0], dist_projected[1]
                            l_total = l["votos"] + l["votosAdicionales"]
                            s_total = s["votos"] + s["votosAdicionales"]
                            margin = l_total - s_total
                            st.info(
                                f"**{l['nombre']}** ({l['votos']:,} → {l_total:,}) "
                                f"aventaja a **{s['nombre']}** ({s['votos']:,} → {s_total:,}) "
                                f"por **{margin:,} votos**"
                            )
                        st.altair_chart(participantes_chart(adj), use_container_width=True)
