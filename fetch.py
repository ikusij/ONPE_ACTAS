from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import json
import time

BASE = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend/"

TOTALES_DISTRITO = BASE + "resumen-general/totales?idAmbitoGeografico={}&idEleccion=10&tipoFiltro=ubigeo_nivel_03&idUbigeoDepartamento={}&idUbigeoProvincia={}&idUbigeoDistrito={}"
TOTALES_PROVINCIA = BASE + "resumen-general/totales?idAmbitoGeografico={}&idEleccion=10&tipoFiltro=ubigeo_nivel_02&idUbigeoDepartamento={}&idUbigeoProvincia={}"
TOTALES_DEPARTAMENTO = BASE + "resumen-general/totales?idAmbitoGeografico={}&idEleccion=10&tipoFiltro=ubigeo_nivel_01&idUbigeoDepartamento={}"
TOTALES_PERU_O_EXTRANJERO = BASE + "resumen-general/totales?idAmbitoGeografico={}&idEleccion=10&tipoFiltro=ambito_geografico"
TOTALES_TODOS = BASE + "resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"

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

def _get(url, retries=5, backoff=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = backoff ** attempt
            print(f"  Retry {attempt+1}/{retries-1} after {wait}s: {e}")
            time.sleep(wait)

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
    totalVotosEmitidos: int
    totalVotosValidos: int
    porcentajeVotosEmitidos: float
    porcentajeVotosValidos: float
    idUbigeoDepartamento: int = 0
    idUbigeoProvincia: int = 0
    idUbigeoDistrito: int = 0
    idUbigeoDistritoElectoral: int = 0

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
    response = _get(TOTALES_DISTRITO.format(id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia, id_ubigeo_distrito))
    return TotalesRaw.from_dict(response.json()["data"])

def fetch_participantes(id_ubigeo_distrito):

    id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia = get_params(id_ubigeo_distrito)
    response = _get(PARTICIPANTES.format(id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia, id_ubigeo_distrito))
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

def generate_all_results(output_path="results.json", max_workers=20):

    with open("nombre_a_ubigeo.json") as f:
        nombre_a_ubigeo = json.load(f)

    distritos = sorted({u for ubigeos in nombre_a_ubigeo.values() for u in ubigeos})
    total = len(distritos)
    results = {}
    completed = 0

    def fetch_one(ubigeo):
        processed = generate_processed(ubigeo)
        return ubigeo, asdict(processed)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, u): u for u in distritos}
        for future in as_completed(futures):
            ubigeo = futures[future]
            completed += 1
            try:
                key, value = future.result()
                results[key] = value
                print(f"[{completed}/{total}] OK: {ubigeo}")
            except Exception as e:
                print(f"[{completed}/{total}] Error {ubigeo}: {e}")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} districts to {output_path}")

def fetch_totales_provincia(id_ubigeo_provincia):

    id_ubigeo_departamento = int(id_ubigeo_provincia) // 10000 * 10000
    id_ambito_geografico = 1 if id_ubigeo_departamento <= 250000 else 2

    response = _get(TOTALES_PROVINCIA.format(id_ambito_geografico, id_ubigeo_departamento, id_ubigeo_provincia))
    return TotalesRaw.from_dict(response.json()["data"])

def generate_all_results_provincia(output_path="results_provincia.json", max_workers=20):

    provincias = {
        prov["nombre"]: prov["ubigeo"]
        for dept in _hierarchy
        for prov in dept["provincias"]
    }

    total = len(provincias)
    results = {}
    completed = 0

    def fetch_one(nombre, ubigeo):
        data = fetch_totales_provincia(ubigeo)
        return f"{nombre} (PROVINCIA)", asdict(data)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, n, u): n for n, u in provincias.items()}
        for future in as_completed(futures):
            nombre = futures[future]
            completed += 1
            try:
                key, value = future.result()
                results[key] = value
                print(f"[{completed}/{total}] OK: {nombre}")
            except Exception as e:
                print(f"[{completed}/{total}] Error {nombre}: {e}")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} provinces to {output_path}")

def fetch_totales_departamento(id_ubigeo_departamento):

    id_ambito_geografico = 1 if int(id_ubigeo_departamento) <= 250000 else 2
    response = _get(TOTALES_DEPARTAMENTO.format(id_ambito_geografico, id_ubigeo_departamento))
    return TotalesRaw.from_dict(response.json()["data"])

def generate_all_results_departamento(output_path="results_departamento.json", max_workers=20):

    departamentos = {
        dept["nombre"]: dept["ubigeo"]
        for dept in _hierarchy
    }

    total = len(departamentos)
    results = {}
    completed = 0

    def fetch_one(nombre, ubigeo):
        data = fetch_totales_departamento(ubigeo)
        return f"{nombre} (DEPARTAMENTO)", asdict(data)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, n, u): n for n, u in departamentos.items()}
        for future in as_completed(futures):
            nombre = futures[future]
            completed += 1
            try:
                key, value = future.result()
                results[key] = value
                print(f"[{completed}/{total}] OK: {nombre}")
            except Exception as e:
                print(f"[{completed}/{total}] Error {nombre}: {e}")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} departments to {output_path}")

def fetch_totales_peru_o_extranjero(id_ambito_geografico):

    response = _get(TOTALES_PERU_O_EXTRANJERO.format(id_ambito_geografico))
    return TotalesRaw.from_dict(response.json()["data"])

def fetch_totales_todos():

    response = _get(TOTALES_TODOS)
    return TotalesRaw.from_dict(response.json()["data"])

def generate_all_results_global(output_path="results_global.json"):

    results = {}

    for label, id_ambito in [("PERU (AMBITO)", 1), ("EXTRANJERO (AMBITO)", 2)]:
        try:
            data = fetch_totales_peru_o_extranjero(id_ambito)
            results[label] = asdict(data)
            print(f"OK: {label}")
        except Exception as e:
            print(f"Error {label}: {e}")

    try:
        data = fetch_totales_todos()
        results["TODOS (ELECCION)"] = asdict(data)
        print("OK: TODOS (ELECCION)")
    except Exception as e:
        print(f"Error TODOS (ELECCION): {e}")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(results)} global entries to {output_path}")

if __name__ == "__main__":
    generate_all_results()
    generate_all_results_provincia()
    generate_all_results_departamento()
    generate_all_results_global()
