"""
ABHIDHAMMA COLAB API RUNNER (v3.0 - GEMINI API + LANGUAGE-AGNOSTIC RŪPA GATE)
=============================================================================
Script operativo per connettere la chiave API di Gemini al `GeminiPerceptionOrgan`
e processare il dataset `opus5-5diseaseconversations.jsonl` senza dipendere da
alcun dizionario o regex linguistica in Python.

Come funziona:
1. Il Gatekeeper Rūpa v2.0 lavora in modalità `language_agnostic_gate=True`:
   misura SOLO la fisica del testo (densità, lunghezza, attrito sigmoideo) senza
   cercare parole chiave in una lingua specifica.
2. I record che superano la soglia fisica entrano nei 17 momenti del `Pañcadvāra-vīthi`.
3. Al Momento 7 (`Santīraṇa`), `GeminiPerceptionOrgan` interroga Gemini (`gemini-3.8-flash`)
   con temperatura controllata dal Canale A e `RESPONSE_JSON_SCHEMA` obbligatorio.
4. Il Kernel Python valida la matrice dei 52 Cetasika (`Sampayoga`), esegue il test
   controfattuale `Natthi-paccaya` nei 7 `Javana`, registra nel `Bhavaṅga-sota` e
   avvia i 12 momenti del `Manodvāra-vīthi` per scoprire le catene transitive.
"""

from __future__ import annotations
import json
import os
import statistics
import time
from typing import Any, Dict, Iterator, List, Optional

from abhidhamma_full_architecture_v8 import (
    AbhidhammaBatchProcessor,
    BatchExecutionSummary,
    BhavangaMemoryStore,
    DeterministicCittaPerceptionOrgan,
    GeminiPerceptionOrgan,
    NeuroSymbolicVithiEngine,
    PatthanaGraph,
    RupaDualStreamPipeline,
)


class JsonlDiseaseStreamReader:
    """
    Lettore streaming O(1) per 'opus5-5diseaseconversations.jsonl'.
    Include anche 'common_mistakes' per mettere alla prova il filtro controfattuale
    Natthi-paccaya di Gemini!
    """
    @classmethod
    def _flatten_field(cls, val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, str):
            return val.strip()
        if isinstance(val, list):
            parts: List[str] = []
            for item in val:
                if isinstance(item, str) and item.strip():
                    parts.append(item.strip())
                elif isinstance(item, dict):
                    content = (
                        item.get("content")
                        or item.get("value")
                        or item.get("text")
                        or item.get("message")
                        or ""
                    )
                    if isinstance(content, str) and content.strip():
                        parts.append(content.strip())
            return " . ".join(parts)
        if isinstance(val, dict):
            return " . ".join(
                f"{v.strip()}" for v in val.values() if isinstance(v, str) and v.strip()
            )
        return str(val).strip()

    @classmethod
    def extract_record_fields(cls, obj: Dict[str, Any], line_idx: int) -> Dict[str, str]:
        disease_name = str(obj.get("name") or obj.get("source_disease") or f"Disease-{line_idx}").strip()
        icd10 = str(obj.get("icd10") or "").strip()
        rec_id = f"{icd10}:{disease_name[:28]}" if icd10 else f"JSONL-{line_idx:06d}:{disease_name[:24]}"

        segments: List[str] = []
        for field_name in (
            "description",
            "executive_summary",
            "patient_scenario",
            "common_mistakes",
            "conversation",
            "drug_interactions",
        ):
            flat = cls._flatten_field(obj.get(field_name))
            if flat:
                if field_name == "common_mistakes":
                    segments.append(f"[Common clinical misconceptions to evaluate: {flat}]")
                else:
                    segments.append(flat)

        full_text = " . ".join(s for s in segments if s)
        return {
            "id": rec_id,
            "disease": disease_name,
            "icd10": icd10 or "N/A",
            "text": full_text.strip()
        }

    @classmethod
    def stream_jsonl(
        cls,
        filepath: str,
        max_records: Optional[int] = None
    ) -> Iterator[Dict[str, str]]:
        with open(filepath, mode="r", encoding="utf-8", errors="replace") as f:
            count = 0
            for line_idx, raw_line in enumerate(f, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rec = cls.extract_record_fields(obj, line_idx)
                        if rec["text"]:
                            yield rec
                            count += 1
                            if max_records is not None and count >= max_records:
                                break
                except json.JSONDecodeError:
                    continue


def resolve_gemini_api_key(explicit_key: Optional[str] = None) -> Optional[str]:
    """
    Recupera la chiave API di Gemini in ordine di sicurezza:
    1. Parametro esplicito passato alla funzione
    2. Variabile d'ambiente `GEMINI_API_KEY` o `GOOGLE_API_KEY`
    3. Segreti di Google Colab (`google.colab.userdata.get('GEMINI_API_KEY')`)
    """
    if explicit_key and not explicit_key.startswith("INSERISCI"):
        return explicit_key.strip()
    env_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if env_key:
        return env_key.strip()
    try:
        from google.colab import userdata  # type: ignore
        for secret_name in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
            try:
                val = userdata.get(secret_name)
                if val:
                    return val.strip()
            except Exception:
                continue
    except ImportError:
        pass
    return None


def run_abhidhamma_gemini_session(
    filepath: str,
    api_key: Optional[str] = None,
    model_name: str = "gemini-3.8-flash",
    max_records: int = 25,
    friction_quantile_gate: float = 0.50,
    output_bhavanga_json: str = "bhavanga_gemini_knowledge_graph.json"
) -> BatchExecutionSummary:
    """
    Esegue la pipeline Neuro-Simbolica completa con `GeminiPerceptionOrgan` attivo:
    - Calibra automaticamente la soglia fisica Rūpa sul quantile scelto (`friction_quantile_gate`),
      in modo da controllare con precisione quante chiamate API effettuare.
    - Attiva `language_agnostic_gate=True` (zero dipendenza da vocabolari o lingue in Python).
    - Stampa il report completo delle leggi dirette, delle deduzioni transitive (`Manodvāra`)
      e delle fallacie cliniche espulse in `JAVANA_3`.
    """
    resolved_key = resolve_gemini_api_key(api_key)
    mode_label = (
        f"NEURO-SIMBOLICA ATTIVA ({model_name} via google-genai)"
        if resolved_key
        else "SIMULAZIONE LOCALE (Nessuna API Key rilevata -> Fallback deterministico)"
    )

    print("=" * 90)
    print("AVVIO SESSIONE ABHIDHAMMA NEURO-SYMBOLIC ENGINE (STRATI 1, 2 & 3)")
    print(f"Modalità Organo Percettivo (Saññā/Santīraṇa): {mode_label}")
    print("=" * 90)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File dataset non trovato: '{filepath}'")

    # 1. Pre-scansione termodinamica ultra-rapida sui `max_records` per fissare il Gatekeeper fisico
    pipeline = RupaDualStreamPipeline()
    records_list = list(JsonlDiseaseStreamReader.stream_jsonl(filepath, max_records=max_records))
    if not records_list:
        raise RuntimeError("Nessun record valido trovato nel file JSONL.")

    frictions = [pipeline.process(r["text"]).channel_a_kinetic.sigmoid_friction for r in records_list]
    sorted_f = sorted(frictions)
    q_idx = min(len(sorted_f) - 1, max(0, int(friction_quantile_gate * (len(sorted_f) - 1))))
    gate_threshold = round(sorted_f[q_idx], 4)

    expected_vithi_calls = sum(1 for f in frictions if f >= gate_threshold)
    print(f" -> Record Caricati dallo Stream:        {len(records_list)}")
    print(f" -> Quantile Gatekeeper Fisico Rūpa:     P{int(friction_quantile_gate*100)} -> Soglia Attrito = {gate_threshold:.4f}")
    print(f" -> Chiamate Previste a Santīraṇa (LLM): {expected_vithi_calls}/{len(records_list)} conversazioni")

    # 2. Inizializzazione del Kernel Abhidhammico e del GeminiPerceptionOrgan
    bhavanga_store = BhavangaMemoryStore()
    organ = GeminiPerceptionOrgan(
        model_name=model_name,
        api_key=resolved_key,
        fallback_organ=DeterministicCittaPerceptionOrgan(),
        max_retries=4,
        min_request_interval_sec=12.5,  # Pacing esatto per Free Tier (5 RPM) -> 0 errori 429!
        memory_store_ref=bhavanga_store
    )
    patthana_graph = PatthanaGraph()
    vithi_engine = NeuroSymbolicVithiEngine(
        perception_organ=organ,
        memory_store=bhavanga_store,
        patthana_graph=patthana_graph
    )
    processor = AbhidhammaBatchProcessor(
        vithi_engine=vithi_engine,
        friction_gate_threshold=gate_threshold,
        min_tokens_for_vithi=5,
        language_agnostic_gate=True  # 100% indipendente dalla lingua!
    )

    t0 = time.time()
    summary = processor.process_records(records_list, text_field="text", id_field="id")
    elapsed = time.time() - t0

    print("\n" + "-" * 90)
    print(f"ESECUZIONE COMPLETATA IN {elapsed:.2f}s")
    print("-" * 90)
    print(f" -> Record Totali Esaminati:              {summary.total_records_scanned}")
    print(f" -> Fast-Path (Sati-Observation):         {summary.fast_path_count}")
    print(f" -> Cicli Completi Pañcadvāra-vīthi (17): {summary.full_vithi_count}")
    if resolved_key:
        print(f" -> Chiamate Gemini API Riuscite/Fallite: {organ.api_calls_succeeded} / {organ.api_calls_failed}")
    print(f" -> Nodi Concettuali nel Bhavaṅga:        {len(bhavanga_store.concept_nodes)}")
    print(f" -> Leggi Causali Consolidate:            {len(bhavanga_store.causal_laws)}")
    print(f" -> Scoperte Transitive (Manodvāra):      {len(summary.manodvara_synthesis_report.transitive_discoveries)}")
    print(f" -> Archi Certificati nel Paṭṭhāna Graph: {len(patthana_graph.edges)}")

    if bhavanga_store.causal_laws:
        print("\n[LEGGI CAUSALI CONSOLIDATE NEL BHAVAṄGA-SOTA (TOP 15 PER PESO PAKATŪPANISSAYA)]")
        sorted_laws = sorted(
            bhavanga_store.causal_laws,
            key=lambda x: (x.is_transitive_deduction, x.pakatupanissaya_weight, x.confirmations_count),
            reverse=True
        )
        for law in sorted_laws[:15]:
            ltype = "TRANSITIVA (Manodvāra)" if law.is_transitive_deduction else "DIRETTA (Pañcadvāra)"
            print(
                f"  * [{law.law_id} | {ltype} | Peso={law.pakatupanissaya_weight:.4f} | Conf={law.confirmations_count}]: "
                f"'{law.cause_label}' ==> '{law.effect_label}'"
            )

    rep = summary.manodvara_synthesis_report
    if rep.expelled_spurious_fallacies:
        print("\n[FALLACIE / MISCONCEPTION ESPULSE DAL TEST CONTROFATTUALE NATTHI-PACCAYA (TOP 10)]")
        for c_lbl, e_lbl, cnt in rep.expelled_spurious_fallacies[:10]:
            print(f"  * [ESPULSO x{cnt}]: '{c_lbl}' -/-> '{e_lbl}'")

    with open(output_bhavanga_json, "w", encoding="utf-8") as f:
        f.write(bhavanga_store.to_json())
    print(f"\n -> Knowledge Graph Persistente salvato in: '{output_bhavanga_json}'")
    print("=" * 90)

    return summary


if __name__ == "__main__":
    # Test di verifica immediata dello script operativo
    demo_file = "test_opus5_schema.jsonl"
    if os.path.exists(demo_file):
        run_abhidhamma_gemini_session(
            filepath=demo_file,
            api_key=None,
            max_records=5,
            friction_quantile_gate=0.0
        )
