from dataclasses import dataclass, asdict
import requests
import json

BASE = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend/"
TOTALES = BASE + "resumen-general/totales?idAmbitoGeografico={}&idEleccion=10&tipoFiltro=ubigeo_nivel_03&idUbigeoDepartamento={}&idUbigeoProvincia={}&idUbigeoDistrito={}"
PARTICIPANTES = BASE + "eleccion-presidencial/participantes-ubicacion-geografica-nombre?idAmbitoGeografico={}&idEleccion=10&tipoFiltro=ubigeo_nivel_03&ubigeoNivel1={}&ubigeoNivel2={}&ubigeoNivel3={}"

with open("hierarchy.json") as _f:
    _hierarchy = json.load(_f)
UBIGEO_TO_NAME = {
    d["ubigeo"]: d["nombre"]
    for dept in _hierarchy
    for prov in dept["provincias"]
    for d in prov["distritos"]
}
UBIGEO_TO_PROVINCIA = {
    d["ubigeo"]: prov["nombre"]
    for dept in _hierarchy
    for prov in dept["provincias"]
    for d in prov["distritos"]
}
UBIGEO_TO_DEPARTAMENTO = {
    d["ubigeo"]: dept["nombre"]
    for dept in _hierarchy
    for prov in dept["provincias"]
    for d in prov["distritos"]
}

HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "priority": "u=1, i",
    "referer": "https://resultadoelectoral.onpe.gob.pe/main/presidenciales",
    "sec-ch-ua": '"Not-A.Brand";v="24", "Chromium";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}

@dataclass
class TotalesRaw:
    
    actasContabilizadas: float
    contabilizadas: int
    totalActas: int
    participacionCiudadana: float
    actasEnviadasJee: float
    enviadasJee: int
    actasPendientesJee: float
    pendientesJee: int
    fechaActualizacion: int
    idUbigeoDepartamento: int
    idUbigeoProvincia: int
    idUbigeoDistrito: int
    idUbigeoDistritoElectoral: int
    totalVotosEmitidos: int
    totalVotosValidos: int
    porcentajeVotosEmitidos: float
    porcentajeVotosValidos: float

    @classmethod
    def from_dict(cls, d: dict) -> "TotalesRaw":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

@dataclass
class ParticipantesRaw:
    
    nombreAgrupacionPolitica: str
    codigoAgrupacionPolitica: str
    nombreCandidato: str
    dniCandidato: str
    totalVotosValidos: int = 0
    porcentajeVotosValidos: float = 0.0
    porcentajeVotosEmitidos: float = 0.0

    @classmethod
    def from_dict(cls, d: dict) -> "ParticipantesRaw":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

@dataclass
class ParticipantesProcessed:

    nombre: str
    porcentajeVotos: float = 0.0
    votos: float = 0.0

    @classmethod
    def from_dict(cls, d: dict) -> "ParticipantesProcessed":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

@dataclass
class Processed:

    distrito: str
    provincia: str
    departamento: str

    totalActas: int

    contabilizadas: int
    actasContabilizadas: float
    
    enviadasJee: int
    actasEnviadasJee: float

    pendientesJee: int
    actasPendientesJee: float

    ParticipantesProcessed: ["ParticipantesProcessed"]

    @classmethod
    def from_dict(cls, d: dict) -> "Processed":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    def pretty_print(self):
        print(f"Actas enviadas JEE: {self.enviadasJee} ({self.actasEnviadasJee}%)")
        print(f"{'Candidato/Partido':<45} {'Votos':>8}  {'% Emitidos':>10}")
        print("-" * 67)
        for p in sorted(self.ParticipantesProcessed, key=lambda x: x.votos, reverse=True):
            print(f"{p.nombre:<45} {p.votos:>8}  {p.porcentajeVotos:>9.2f}%")

def get_params(id_ubigeo_distrito):

    id_ubigeo_departamento = int(id_ubigeo_distrito) // 10000 * 10000
    id_ubigeo_provincia = int(id_ubigeo_distrito) // 100 * 100
    id_ambito_geografico = 1 if id_ubigeo_departamento <= 250000 else 2

    return (str(id_ambito_geografico), str(id_ubigeo_departamento), str(id_ubigeo_provincia))

def fetch_totales(id_ubigeo_distrito):

    id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia = get_params(id_ubigeo_distrito)
    response = requests.get(TOTALES.format(id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia, id_ubigeo_distrito), headers=HEADERS)
    response.raise_for_status()
    
    return TotalesRaw.from_dict(response.json()["data"])

def fetch_participantes(id_ubigeo_distrito):

    id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia = get_params(id_ubigeo_distrito)
    response = requests.get(PARTICIPANTES.format(id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia, id_ubigeo_distrito), headers=HEADERS)
    response.raise_for_status()
    
    return [ParticipantesRaw.from_dict(item) for item in response.json()["data"]]

def compute_participantes_processed(participante):

    item = {
        "nombre": participante.nombreCandidato if participante.nombreCandidato else participante.nombreAgrupacionPolitica,
        "porcentajeVotos": participante.porcentajeVotosEmitidos,
        "votos": participante.totalVotosValidos
    }

    return ParticipantesProcessed.from_dict(item)

def generate_processed(id_ubigeo_distrito):

    totales_data = fetch_totales(id_ubigeo_distrito)
    participantes_data = fetch_participantes(id_ubigeo_distrito)

    participantes_Processed = [compute_participantes_processed(item) for item in participantes_data]

    item = {
        "distrito": UBIGEO_TO_NAME.get(id_ubigeo_distrito, id_ubigeo_distrito),
        "provincia": UBIGEO_TO_PROVINCIA.get(id_ubigeo_distrito, id_ubigeo_distrito),
        "departamento": UBIGEO_TO_DEPARTAMENTO.get(id_ubigeo_distrito, id_ubigeo_distrito),
        "totalActas": totales_data.totalActas,
        "contabilizadas": totales_data.contabilizadas,
        "actasContabilizadas": totales_data.actasContabilizadas,
        "enviadasJee": totales_data.enviadasJee,
        "actasEnviadasJee": totales_data.actasEnviadasJee,
        "pendientesJee": totales_data.pendientesJee,
        "actasPendientesJee": totales_data.actasPendientesJee,
        "ParticipantesProcessed": participantes_Processed
    }

    return Processed.from_dict(item)

def generate_all_results(output_path="results.json"):
    
    with open("nombre_a_ubigeo.json") as f:
        nombre_a_ubigeo = json.load(f)

    distritos = sorted({u for ubigeos in nombre_a_ubigeo.values() for u in ubigeos})

    results = {}
    for i, ubigeo in enumerate(distritos):
        
        try:
            print(f"[{i+1}/{len(distritos)}] Processing: {ubigeo}")
            processed = generate_processed(ubigeo)
            results[ubigeo] = asdict(processed)
        except Exception as e:
            print(f"Error fetching {ubigeo}: {e}")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} districts to {output_path}")

if __name__ == "__main__":
    generate_all_results()
