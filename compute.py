from dataclasses import dataclass, asdict
from fetch import ParticipantesProcessed
from pprint import pprint
import json

@dataclass
class ParticipantesOutput(ParticipantesProcessed):

    votosAdicionales: float = 0.0

    @classmethod
    def from_dict(cls, d: dict) -> "ParticipantesOutput":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

@dataclass
class Output:

    distrito: str
    provincia: str
    departamento: str
    totalActas: int
    contabilizadas: int
    actasContabilizadas: float
    enviadasJee: float
    actasEnviadasJee: float
    pendientesJee: int
    actasPendientesJee: float
    participantesOutput: list

    @classmethod
    def from_dict(cls, d: dict) -> "Output":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

def generate_output(data, votos_por_acta, filtro):

    participantes = []
    for p in data["ParticipantesProcessed"]:
        
        if p["nombre"] not in filtro:
            continue

        votos_adicionales = int(votos_por_acta * data["enviadasJee"] * p["porcentajeVotos"] / 100)

        participantes.append(ParticipantesOutput(
            nombre=p["nombre"],
            porcentajeVotos=p["porcentajeVotos"],
            votos=p["votos"],
            votosAdicionales=votos_adicionales,
        ))

    return Output(
        distrito=data["distrito"],
        provincia=data["provincia"],
        departamento=data["departamento"],
        totalActas=data["totalActas"],
        contabilizadas=data["contabilizadas"],
        actasContabilizadas=data["actasContabilizadas"],
        enviadasJee=data["enviadasJee"],
        actasEnviadasJee=data["actasEnviadasJee"],
        pendientesJee=data["pendientesJee"],
        actasPendientesJee=data["actasPendientesJee"],
        participantesOutput=participantes,
    )

def compute_all(votos_por_acta, filtro):
    with open("results.json") as f:
        results = json.load(f)

    computed = {}
    for ubigeo, data in results.items():
        computed[ubigeo] = asdict(generate_output(data, votos_por_acta, filtro))

    with open("results_additional_votes.json", "w") as f:
        json.dump(computed, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(computed)} districts to results_additional_votes.json")

if __name__ == "__main__":
    compute_all(220, ["ROBERTO HELBERT SANCHEZ PALOMINO", "RAFAEL BERNARDO LÓPEZ ALIAGA CAZORLA"])
