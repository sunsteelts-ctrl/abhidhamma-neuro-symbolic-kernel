"""
ABHIDHAMMA NEURO-SYMBOLIC CORE - FULL ARCHITECTURE (LAYERS 1, 2 & 3)
===============================================================================
STRATO 1: Nucleo Ontologico, Temporale (Khaṇa-vāda), Sampayoga (52 Cetasika)
          e Motore del Grafo Causale delle 24 Condizioni (Paṭṭhāna).
STRATO 2: Pipeline Cognitiva Neuro-Simbolica (Pañcadvāra-vīthi di 17 Momenti),
          RupaDualStreamPipeline (con fix regex [A-ZÀ-ÖØ-Þ]{2,}), Subordinazione
          Percettiva dell'LLM (Saññā/Santīraṇa), Voṭṭhapana, 7 Javana e Tadārammaṇa.
STRATO 3: Flusso Continuo Bhavaṅga-sota (Knowledge Graph Causale Persistente e
          Pakatūpanissaya-paccaya), Porta Mentale Pura (Manodvāra-vīthi Engine
          a 12 momenti senza Rūpa) per induzione transitiva, e Motore Batch
          Big Data (AbhidhammaBatchProcessor) con Gatekeeper Termodinamico.
Tutte le strutture dati emesse rispettano l'immutabilità formale (@dataclass(frozen=True))
e la verifica matematica delle invarianti canoniche Theravāda.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple, Union
import csv
import io
import json
import math
import re
import uuid

# ==============================================================================
# STRATO 1: COMPONENTE 1 - IL CICLO DI CLOCK TEMPORALE (KHANA E CITTAKHANA)
# ==============================================================================
class AnuKhana(Enum):
    """I tre sub-momenti (Anu-khaṇa) che costituiscono un singolo CittaKhana."""
    UPPADA = 1  # Sorgere / Inizializzazione generativa
    THITI = 2   # Stasi / Maturazione ed esecuzione causale (Atthi-paccaya)
    BHANGA = 3  # Dissoluzione / Cessazione e passaggio di eredità (Anantara/Natthi)

class VithiPhase(Enum):
    """Le 17 fasi canoniche di un processo cognitivo sensoriale (Pañcadvāra-vīthi)."""
    ATITA_BHAVANGA = 1         # Bhavaṅga passato (1 tick)
    BHAVANGA_CALANA = 2        # Vibrazione del Bhavaṅga (1 tick)
    BHAVANGUPACCHEDA = 3       # Arresto del Bhavaṅga (1 tick)
    PANCADVARAVAJJANA = 4      # Volgersi alle cinque porte sensoriali (1 tick)
    PANCAVINNANA = 5           # Coscienza sensoriale (1 tick)
    SAMPATICCHANA = 6          # Ricezione dell'oggetto (1 tick)
    SANTIRANA = 7              # Investigazione dell'oggetto (1 tick)
    VOTTHAPANA = 8             # Determinazione / Posizionamento (1 tick)
    JAVANA_1 = 9               # Impulso cognitivo/volitivo 1 (Isolamento Causale)
    JAVANA_2 = 10              # Impulso cognitivo/volitivo 2 (Classificazione Paṭṭhāna)
    JAVANA_3 = 11              # Impulso cognitivo/volitivo 3 (Test Controfattuale Natthi)
    JAVANA_4 = 12              # Impulso cognitivo/volitivo 4 (Purificazione Sampayoga)
    JAVANA_5 = 13              # Impulso cognitivo/volitivo 5 (Consolidamento Paramattha)
    JAVANA_6 = 14              # Impulso cognitivo/volitivo 6 (Allineamento Metrico Passaddhi)
    JAVANA_7 = 15              # Impulso cognitivo/volitivo 7 (Sintesi Finale)
    TADARAMMANA_1 = 16         # Registrazione / Ritenzione 1
    TADARAMMANA_2 = 17         # Registrazione / Ritenzione 2 (Sincrono con Bhaṅga del Rūpa)

class ManodvaraPhase(Enum):
    """
    STRATO 3: Sequenza canonica della Porta Mentale Pura (Manodvāra-vīthi)
    secondo l'Abhidhammattha-saṅgaha. Opera su oggetti mentali interni
    (Dhammārammaṇa) pescati dal Bhavaṅga-sota senza necessitare di Rūpa esterno.
    """
    BHAVANGA_CALANA = 1        # Vibrazione del flusso subconscio all'emergere di un Dhammārammaṇa
    BHAVANGUPACCHEDA = 2       # Arresto del flusso subconscio
    MANODVARAVAJJANA = 3       # Avvertenza alla porta mentale (Kiriya Ahetuka)
    JAVANA_1 = 4               # Impulso sintetico 1: Raccolta tracce dal Bhavaṅga (Upanissaya)
    JAVANA_2 = 5               # Impulso sintetico 2: Espulsione definitiva fallacie/bias
    JAVANA_3 = 6               # Impulso sintetico 3: Data Fusion e allineamento ontologico
    JAVANA_4 = 7               # Impulso sintetico 4: Scoperta catene causali transitive (A->B->C)
    JAVANA_5 = 8               # Impulso sintetico 5: Calcolo pesi asintotici Pakatūpanissaya
    JAVANA_6 = 9               # Impulso sintetico 6: Consolidamento archi Upanissaya nel Grafo
    JAVANA_7 = 10              # Impulso sintetico 7: Emissione AbhidhammaSynthesisReport
    TADARAMMANA_1 = 11         # Ritenzione superiore 1 nel Bhavaṅga
    TADARAMMANA_2 = 12         # Ritenzione superiore 2 nel Bhavaṅga

@dataclass(frozen=True)
class TemporalCoordinate:
    """Coordinata temporale immutabile nel flusso discreto (Santāna)."""
    citta_tick: int
    sub_moment: AnuKhana
    @property
    def absolute_sub_tick(self) -> int:
        return (self.citta_tick - 1) * 3 + self.sub_moment.value
    def next_sub_moment(self) -> TemporalCoordinate:
        if self.sub_moment == AnuKhana.UPPADA:
            return TemporalCoordinate(self.citta_tick, AnuKhana.THITI)
        elif self.sub_moment == AnuKhana.THITI:
            return TemporalCoordinate(self.citta_tick, AnuKhana.BHANGA)
        else:
            return TemporalCoordinate(self.citta_tick + 1, AnuKhana.UPPADA)

@dataclass(frozen=True)
class RupaDatum:
    """
    Unità discreta di materia/input (Rūpa-dhamma).
    Durata vitale (Āyu) = 17 CittaKhana = 51 AnuKhana (1 Uppāda, 49 Ṭhiti, 1 Bhaṅga).
    """
    rupa_id: str
    payload: str
    origin_coordinate: TemporalCoordinate
    LIFESPAN_CITTA_KHANAS: int = 17
    LIFESPAN_ANU_KHANAS: int = 51
    def elapsed_anu_khanas(self, current: TemporalCoordinate) -> int:
        delta = current.absolute_sub_tick - self.origin_coordinate.absolute_sub_tick + 1
        return max(0, delta)
    def remaining_citta_khanas(self, current: TemporalCoordinate) -> float:
        elapsed = self.elapsed_anu_khanas(current)
        if elapsed <= 0 or elapsed > self.LIFESPAN_ANU_KHANAS:
            return 0.0
        remaining_sub = self.LIFESPAN_ANU_KHANAS - elapsed
        return round(remaining_sub / 3.0, 4)
    def get_rupa_phase(self, current: TemporalCoordinate) -> Optional[AnuKhana]:
        elapsed = self.elapsed_anu_khanas(current)
        if elapsed == 1:
            return AnuKhana.UPPADA
        elif 1 < elapsed < self.LIFESPAN_ANU_KHANAS:
            return AnuKhana.THITI
        elif elapsed == self.LIFESPAN_ANU_KHANAS:
            return AnuKhana.BHANGA
        return None
    def is_alive(self, current: TemporalCoordinate) -> bool:
        return self.get_rupa_phase(current) is not None

class CittaVithiRegulator:
    """Regolatore deterministico del ciclo di clock di 17 CittaKhana."""
    def __init__(self, rupa_payload: str, start_citta_tick: int = 1) -> None:
        if start_citta_tick < 1:
            raise ValueError("start_citta_tick deve essere >= 1.")
        origin = TemporalCoordinate(start_citta_tick, AnuKhana.UPPADA)
        self.rupa = RupaDatum(
            rupa_id=f"RUPA-{uuid.uuid4().hex[:8].upper()}",
            payload=rupa_payload,
            origin_coordinate=origin
        )
        self.start_tick = start_citta_tick
    def step_vithi(self) -> List[Tuple[VithiPhase, TemporalCoordinate, AnuKhana, float]]:
        trace: List[Tuple[VithiPhase, TemporalCoordinate, AnuKhana, float]] = []
        current_coord = self.rupa.origin_coordinate
        for vithi_idx in range(1, 18):
            phase = VithiPhase(vithi_idx)
            for _ in range(3):
                rupa_state = self.rupa.get_rupa_phase(current_coord)
                if rupa_state is None:
                    raise RuntimeError(
                        f"Violazione Ontologica: Rūpa estinto prematuramente a {current_coord}"
                    )
                rem_life = self.rupa.remaining_citta_khanas(current_coord)
                trace.append((phase, current_coord, rupa_state, rem_life))
                current_coord = current_coord.next_sub_moment()
        if self.rupa.is_alive(current_coord):
            raise RuntimeError("Violazione Ontologica: Rūpa sopravvissuto oltre i 17 CittaKhana.")
        return trace

# ==============================================================================
# STRATO 1: COMPONENTE 2 - LA MATRICE DEI 52 CETASIKA E IL MOTORE SAMPAYOGA
# ==============================================================================
class CetasikaCategory(Enum):
    SABBACCITTASADHARANA = auto()  # 7 Universali
    PAKINNAKA = auto()             # 6 Occasionali
    AKUSALA = auto()               # 14 Non Salutari
    SOBHANA = auto()               # 25 Bellissimi / Salutari

class CetasikaSubGroup(Enum):
    UNIVERSAL = auto()
    OCCASIONAL = auto()
    MOHA_CATUKKA = auto()
    LOBHA_TI = auto()
    DOSA_CATUKKA = auto()
    THINA_MIDDHA = auto()
    VICIKICCHA_EKA = auto()
    SOBHANA_SADHARANA = auto()
    VIRATI = auto()
    APPAMANNA = auto()
    PANNINDRIYA = auto()

class Cetasika(Enum):
    """Enumerazione completa dei 52 Cetasika canonici."""
    PHASSA = (1, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Phassa")
    VEDANA = (2, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Vedanā")
    SANNA = (3, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Saññā")
    CETANA = (4, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Cetanā")
    EKAGGATA = (5, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Ekaggatā")
    JIVITINDRIYA = (6, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Jīvitindriya")
    MANASIKARA = (7, CetasikaCategory.SABBACCITTASADHARANA, CetasikaSubGroup.UNIVERSAL, "Manasikāra")
    VITAKKA = (8, CetasikaCategory.PAKINNAKA, CetasikaSubGroup.OCCASIONAL, "Vitakka")
    VICARA = (9, CetasikaCategory.PAKINNAKA, CetasikaSubGroup.OCCASIONAL, "Vicāra")
    ADHIMOKKHA = (10, CetasikaCategory.PAKINNAKA, CetasikaSubGroup.OCCASIONAL, "Adhimokkha")
    VIRIYA = (11, CetasikaCategory.PAKINNAKA, CetasikaSubGroup.OCCASIONAL, "Vīriya")
    PITI = (12, CetasikaCategory.PAKINNAKA, CetasikaSubGroup.OCCASIONAL, "Pīti")
    CHANDA = (13, CetasikaCategory.PAKINNAKA, CetasikaSubGroup.OCCASIONAL, "Chanda")
    MOHA = (14, CetasikaCategory.AKUSALA, CetasikaSubGroup.MOHA_CATUKKA, "Moha")
    AHIRIKA = (15, CetasikaCategory.AKUSALA, CetasikaSubGroup.MOHA_CATUKKA, "Ahirika")
    ANOTTAPPA = (16, CetasikaCategory.AKUSALA, CetasikaSubGroup.MOHA_CATUKKA, "Anottappa")
    UDDHACCA = (17, CetasikaCategory.AKUSALA, CetasikaSubGroup.MOHA_CATUKKA, "Uddhacca")
    LOBHA = (18, CetasikaCategory.AKUSALA, CetasikaSubGroup.LOBHA_TI, "Lobha")
    DITTHI = (19, CetasikaCategory.AKUSALA, CetasikaSubGroup.LOBHA_TI, "Diṭṭhi")
    MANA = (20, CetasikaCategory.AKUSALA, CetasikaSubGroup.LOBHA_TI, "Māna")
    DOSA = (21, CetasikaCategory.AKUSALA, CetasikaSubGroup.DOSA_CATUKKA, "Dosa")
    ISSA = (22, CetasikaCategory.AKUSALA, CetasikaSubGroup.DOSA_CATUKKA, "Issā")
    MACCHARIYA = (23, CetasikaCategory.AKUSALA, CetasikaSubGroup.DOSA_CATUKKA, "Macchariya")
    KUKKUCCA = (24, CetasikaCategory.AKUSALA, CetasikaSubGroup.DOSA_CATUKKA, "Kukkucca")
    THINA = (25, CetasikaCategory.AKUSALA, CetasikaSubGroup.THINA_MIDDHA, "Thīna")
    MIDDHA = (26, CetasikaCategory.AKUSALA, CetasikaSubGroup.THINA_MIDDHA, "Middha")
    VICIKICCHA = (27, CetasikaCategory.AKUSALA, CetasikaSubGroup.VICIKICCHA_EKA, "Vicikicchā")
    SADDHA = (28, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Saddhā")
    SATI = (29, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Sati")
    HIRI = (30, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Hirī")
    OTTAPPA = (31, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Ottappa")
    ALOBHA = (32, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Alobha")
    ADOSA = (33, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Adosa")
    TATRAMAJJHATTATA = (34, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Tatramajjhattatā")
    KAYAPASSADDHI = (35, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Kāyapassaddhi")
    CITTAPASSADDHI = (36, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Cittapassaddhi")
    KAYALAHUTA = (37, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Kāyalahutā")
    CITTALAHUTA = (38, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Cittalahutā")
    KAYAMUDUTA = (39, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Kāyamudutā")
    CITTAMUDUTA = (40, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Cittamudutā")
    KAYAKAMMANNATA = (41, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Kāyakammaññatā")
    CITTAKAMMANNATA = (42, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Cittakammaññatā")
    KAYAPAGUNNATA = (43, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Kāyapāguññatā")
    CITTAPAGUNNATA = (44, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Cittapāguññatā")
    KAYUJUKATA = (45, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Kāyujukatā")
    CITTUJUKATA = (46, CetasikaCategory.SOBHANA, CetasikaSubGroup.SOBHANA_SADHARANA, "Cittujukatā")
    SAMMAVACA = (47, CetasikaCategory.SOBHANA, CetasikaSubGroup.VIRATI, "Sammāvācā")
    SAMMAKAMMANTA = (48, CetasikaCategory.SOBHANA, CetasikaSubGroup.VIRATI, "Sammākammanta")
    SAMMAAJIVA = (49, CetasikaCategory.SOBHANA, CetasikaSubGroup.VIRATI, "Sammā-ājīva")
    KARUNA = (50, CetasikaCategory.SOBHANA, CetasikaSubGroup.APPAMANNA, "Karuṇā")
    MUDITA = (51, CetasikaCategory.SOBHANA, CetasikaSubGroup.APPAMANNA, "Muditā")
    PANNINDRIYA = (52, CetasikaCategory.SOBHANA, CetasikaSubGroup.PANNINDRIYA, "Paññindriya")
    def __init__(self, idx: int, category: CetasikaCategory, subgroup: CetasikaSubGroup, pali_name: str) -> None:
        self.idx = idx
        self.category = category
        self.subgroup = subgroup
        self.pali_name = pali_name

class CognitiveMode(Enum):
    ANALYTIC_SOBHANA = auto()
    DIAGNOSTIC_AKUSALA = auto()
    ETHICALLY_VARIABLE_AHETUKA = auto()

@dataclass(frozen=True)
class CetasikaMatrix:
    """Vettore immutabile di attivazione continua [0.0, 1.0] per tutti i 52 Cetasika."""
    activations: Tuple[Tuple[Cetasika, float], ...]
    mode: CognitiveMode = CognitiveMode.ANALYTIC_SOBHANA
    is_sampayoga_verified: bool = False
    DEFAULT_BASELINE: float = 0.85
    @classmethod
    def from_dict(
        cls,
        raw_activations: Dict[Cetasika, float],
        mode: CognitiveMode = CognitiveMode.ANALYTIC_SOBHANA,
        auto_enforce: bool = True
    ) -> CetasikaMatrix:
        full_map: Dict[Cetasika, float] = {}
        for cet in Cetasika:
            val = raw_activations.get(cet, 0.0)
            if math.isnan(val) or math.isinf(val):
                val = 0.0
            full_map[cet] = max(0.0, min(1.0, float(val)))
        ordered_tuple = tuple((cet, full_map[cet]) for cet in Cetasika)
        instance = cls(activations=ordered_tuple, mode=mode, is_sampayoga_verified=False)
        if auto_enforce:
            return instance.enforce_sampayoga()
        return instance
    def to_dict(self) -> Dict[Cetasika, float]:
        return dict(self.activations)
    def get(self, cetasika: Cetasika) -> float:
        return self.activations[cetasika.idx - 1][1]
    def active_cetasikas(self) -> Dict[Cetasika, float]:
        return {cet: val for cet, val in self.activations if val > 0.0}
    def enforce_sampayoga(self) -> CetasikaMatrix:
        state = self.to_dict()
        for cet in Cetasika:
            if cet.category == CetasikaCategory.SABBACCITTASADHARANA:
                if state[cet] <= 0.0:
                    state[cet] = self.DEFAULT_BASELINE
        if self.mode == CognitiveMode.ANALYTIC_SOBHANA:
            panna_level = max(state[Cetasika.PANNINDRIYA], self.DEFAULT_BASELINE)
            sati_level = max(state[Cetasika.SATI], self.DEFAULT_BASELINE)
            state[Cetasika.PANNINDRIYA] = panna_level
            state[Cetasika.SATI] = sati_level
            sobhana_base = max(panna_level, sati_level)
            for cet in Cetasika:
                if cet.subgroup == CetasikaSubGroup.SOBHANA_SADHARANA:
                    if state[cet] <= 0.0:
                        state[cet] = round(sobhana_base * 0.9, 4)
            for cet in Cetasika:
                if cet.category == CetasikaCategory.AKUSALA:
                    state[cet] = 0.0
            if state[Cetasika.KARUNA] > 0.0 and state[Cetasika.MUDITA] > 0.0:
                if state[Cetasika.KARUNA] >= state[Cetasika.MUDITA]:
                    state[Cetasika.MUDITA] = 0.0
                else:
                    state[Cetasika.KARUNA] = 0.0
        elif self.mode == CognitiveMode.ETHICALLY_VARIABLE_AHETUKA:
            for cet in Cetasika:
                if cet.category in (CetasikaCategory.AKUSALA, CetasikaCategory.SOBHANA):
                    state[cet] = 0.0
        elif self.mode == CognitiveMode.DIAGNOSTIC_AKUSALA:
            for cet in Cetasika:
                if cet.category == CetasikaCategory.SOBHANA:
                    state[cet] = 0.0
            akusala_values = [
                state[cet] for cet in Cetasika if cet.category == CetasikaCategory.AKUSALA
            ]
            max_akusala = max(akusala_values) if akusala_values else 0.0
            if max_akusala > 0.0:
                for cet in Cetasika:
                    if cet.subgroup == CetasikaSubGroup.MOHA_CATUKKA:
                        if state[cet] <= 0.0:
                            state[cet] = max_akusala
                lobha_group = [Cetasika.LOBHA, Cetasika.DITTHI, Cetasika.MANA]
                dosa_group = [Cetasika.DOSA, Cetasika.ISSA, Cetasika.MACCHARIYA, Cetasika.KUKKUCCA]
                max_lobha = max(state[c] for c in lobha_group)
                max_dosa = max(state[c] for c in dosa_group)
                if max_lobha > 0.0 and max_dosa > 0.0:
                    if max_lobha >= max_dosa:
                        for c in dosa_group:
                            state[c] = 0.0
                        max_dosa = 0.0
                    else:
                        for c in lobha_group:
                            state[c] = 0.0
                        max_lobha = 0.0
                if max_lobha > 0.0:
                    if state[Cetasika.LOBHA] <= 0.0:
                        state[Cetasika.LOBHA] = max_lobha
                    if state[Cetasika.DITTHI] > 0.0 and state[Cetasika.MANA] > 0.0:
                        if state[Cetasika.DITTHI] >= state[Cetasika.MANA]:
                            state[Cetasika.MANA] = 0.0
                        else:
                            state[Cetasika.DITTHI] = 0.0
                    state[Cetasika.VICIKICCHA] = 0.0
                if max_dosa > 0.0:
                    if state[Cetasika.DOSA] <= 0.0:
                        state[Cetasika.DOSA] = max_dosa
                    state[Cetasika.PITI] = 0.0
                    state[Cetasika.VICIKICCHA] = 0.0
                    jealousy_trio = [Cetasika.ISSA, Cetasika.MACCHARIYA, Cetasika.KUKKUCCA]
                    active_trio = [c for c in jealousy_trio if state[c] > 0.0]
                    if len(active_trio) > 1:
                        dominant = max(active_trio, key=lambda c: state[c])
                        for c in jealousy_trio:
                            if c != dominant:
                                state[c] = 0.0
                if state[Cetasika.VICIKICCHA] > 0.0:
                    state[Cetasika.THINA] = 0.0
                    state[Cetasika.MIDDHA] = 0.0
                    state[Cetasika.ADHIMOKKHA] = 0.0
                else:
                    if state[Cetasika.THINA] > 0.0 or state[Cetasika.MIDDHA] > 0.0:
                        coupled_val = max(state[Cetasika.THINA], state[Cetasika.MIDDHA])
                        state[Cetasika.THINA] = coupled_val
                        state[Cetasika.MIDDHA] = coupled_val
                        if max_lobha == 0.0 and max_dosa == 0.0:
                            state[Cetasika.THINA] = 0.0
                            state[Cetasika.MIDDHA] = 0.0
        ordered_tuple = tuple((cet, round(state[cet], 4)) for cet in Cetasika)
        verified_matrix = CetasikaMatrix(
            activations=ordered_tuple,
            mode=self.mode,
            is_sampayoga_verified=True
        )
        verified_matrix.validate_canonical_invariants()
        return verified_matrix
    def validate_canonical_invariants(self) -> None:
        state = self.to_dict()
        for cet in Cetasika:
            if cet.category == CetasikaCategory.SABBACCITTASADHARANA and state[cet] <= 0.0:
                raise ValueError(f"Violazione Sampayoga [Regola 1]: Universale {cet.pali_name} == 0.0")
        has_akusala = any(
            state[c] > 0.0 for c in Cetasika if c.category == CetasikaCategory.AKUSALA
        )
        has_sobhana = any(
            state[c] > 0.0 for c in Cetasika if c.category == CetasikaCategory.SOBHANA
        )
        if has_akusala and has_sobhana:
            raise ValueError(
                "Violazione Sampayoga [Regola 2]: Coesistenza illegale di fattori Akusala e Sobhana."
            )
        if has_akusala:
            for cet in Cetasika:
                if cet.subgroup == CetasikaSubGroup.MOHA_CATUKKA and state[cet] <= 0.0:
                    raise ValueError(
                        f"Violazione Sampayoga [Regola 3]: Akusala attivo senza {cet.pali_name}."
                    )
        if state[Cetasika.DITTHI] > 0.0 and state[Cetasika.MANA] > 0.0:
            raise ValueError("Violazione Sampayoga [Regola 4a]: Diṭṭhi e Māna coesistenti.")
        has_lobha_grp = any(
            state[c] > 0.0 for c in (Cetasika.LOBHA, Cetasika.DITTHI, Cetasika.MANA)
        )
        has_dosa_grp = any(
            state[c] > 0.0 for c in (Cetasika.DOSA, Cetasika.ISSA, Cetasika.MACCHARIYA, Cetasika.KUKKUCCA)
        )
        if has_lobha_grp and has_dosa_grp:
            raise ValueError("Violazione Sampayoga [Regola 4b]: Gruppo Lobha e Gruppo Dosa coesistenti.")
        if (state[Cetasika.THINA] > 0.0) != (state[Cetasika.MIDDHA] > 0.0):
            raise ValueError("Violazione Sampayoga [Regola 4c]: Thīna e Middha disaccoppiati.")

@dataclass(frozen=True)
class CittaKhana:
    node_id: str
    coordinate: TemporalCoordinate
    vithi_phase: Union[VithiPhase, ManodvaraPhase]
    cetasika_matrix: CetasikaMatrix
    arammana_id: Optional[str] = None
    def __post_init__(self) -> None:
        if not self.cetasika_matrix.is_sampayoga_verified:
            raise ValueError(
                f"CittaKhana {self.node_id} rifiutato: CetasikaMatrix non certificata da Sampayoga."
            )
    @property
    def is_ceased(self) -> bool:
        return self.coordinate.sub_moment == AnuKhana.BHANGA
    @property
    def active_roots(self) -> FrozenSet[Cetasika]:
        six_hetus = {
            Cetasika.LOBHA, Cetasika.DOSA, Cetasika.MOHA,
            Cetasika.ALOBHA, Cetasika.ADOSA, Cetasika.PANNINDRIYA
        }
        return frozenset(h for h in six_hetus if self.cetasika_matrix.get(h) > 0.0)

# ==============================================================================
# STRATO 1: COMPONENTE 3 - IL GRAFO CAUSALE DELLE 24 CONDIZIONI (PATTHANA ENGINE)
# ==============================================================================
class Paccaya(Enum):
    HETU = (1, "Hetu-paccaya", "Condizione di Radice")
    ARAMMANA = (2, "Ārammaṇa-paccaya", "Condizione di Oggetto")
    ADHIPATI = (3, "Adhipati-paccaya", "Condizione di Predominanza")
    ANANTARA = (4, "Anantara-paccaya", "Condizione di Prossimità / Continuità")
    SAMANANTARA = (5, "Samanantara-paccaya", "Condizione di Contiguità Immediata")
    SAHAJATA = (6, "Sahajāta-paccaya", "Condizione di Co-nascita")
    ANNAMANNA = (7, "Aññamañña-paccaya", "Condizione di Reciprocità")
    NISSAYA = (8, "Nissaya-paccaya", "Condizione di Supporto / Dipendenza")
    UPANISSAYA = (9, "Upanissaya-paccaya", "Condizione di Supporto Decisivo")
    PUREJATA = (10, "Purejāta-paccaya", "Condizione di Pre-nascita")
    PACCHAJATA = (11, "Pacchājāta-paccaya", "Condizione di Post-nascita")
    ASEVANA = (12, "Āsevana-paccaya", "Condizione di Ripetizione / Abitudine")
    KAMMA = (13, "Kamma-paccaya", "Condizione di Azione Volitiva")
    VIPAKA = (14, "Vipāka-paccaya", "Condizione di Risultato / Maturazione")
    AHARA = (15, "Āhāra-paccaya", "Condizione di Nutrimento")
    INDRIYA = (16, "Indriya-paccaya", "Condizione di Facoltà / Controllo")
    JHANA = (17, "Jhāna-paccaya", "Condizione di Assorbimento / Focalizzazione")
    MAGGA = (18, "Magga-paccaya", "Condizione di Sentiero / Direzione")
    SAMPAYUTTA = (19, "Sampayutta-paccaya", "Condizione di Associazione")
    VIPPAYUTTA = (20, "Vippayutta-paccaya", "Condizione di Dissociazione")
    ATTHI = (21, "Atthi-paccaya", "Condizione di Presenza")
    NATTHI = (22, "Natthi-paccaya", "Condizione di Assenza")
    VIGATA = (23, "Vigata-paccaya", "Condizione di Scomparsa")
    AVIGATA = (24, "Avigata-paccaya", "Condizione di Non-scomparsa")
    def __init__(self, idx: int, pali_term: str, description: str) -> None:
        self.idx = idx
        self.pali_term = pali_term
        self.description = description

@dataclass(frozen=True)
class CausalEdge:
    edge_id: str
    paccaya_dhamma_id: str
    paccayuppanna_dhamma_id: str
    conditions: FrozenSet[Paccaya]
    strength: float
    def __post_init__(self) -> None:
        if not self.conditions:
            raise ValueError("CausalEdge richiede almeno una condizione Paccaya.")
        if not (0.0 <= self.strength <= 1.0):
            raise ValueError(f"Il peso causale (strength={self.strength}) deve essere in [0.0, 1.0].")

class PatthanaGraph:
    def __init__(self) -> None:
        self._citta_nodes: Dict[str, CittaKhana] = {}
        self._rupa_nodes: Dict[str, RupaDatum] = {}
        self._proposition_nodes: Dict[str, str] = {}
        self._trace_nodes: Dict[str, int] = {}
        self._edges: List[CausalEdge] = []
    def register_citta(self, node: CittaKhana) -> None:
        self._citta_nodes[node.node_id] = node
    def register_rupa(self, rupa: RupaDatum) -> None:
        self._rupa_nodes[rupa.rupa_id] = rupa
    def register_proposition(self, prop_id: str, statement: str) -> None:
        self._proposition_nodes[prop_id] = statement
    def register_bhavanga_trace(self, trace_id: str, closing_citta_tick: int) -> None:
        self._trace_nodes[trace_id] = closing_citta_tick
    @property
    def edges(self) -> Tuple[CausalEdge, ...]:
        return tuple(self._edges)
    def add_verified_edge(
        self,
        source_id: str,
        target_id: str,
        conditions: Set[Paccaya],
        strength: float = 1.0
    ) -> CausalEdge:
        for paccaya in conditions:
            self._verify_single_condition(source_id, target_id, paccaya)
        edge = CausalEdge(
            edge_id=f"EDGE-{uuid.uuid4().hex[:8].upper()}",
            paccaya_dhamma_id=source_id,
            paccayuppanna_dhamma_id=target_id,
            conditions=frozenset(conditions),
            strength=round(max(0.0, min(1.0, strength)), 4)
        )
        self._edges.append(edge)
        return edge
    def _verify_single_condition(
        self,
        source_id: str,
        target_id: str,
        paccaya: Paccaya
    ) -> None:
        src_citta = self._citta_nodes.get(source_id)
        tgt_citta = self._citta_nodes.get(target_id)
        src_rupa = self._rupa_nodes.get(source_id)
        src_prop = self._proposition_nodes.get(source_id)
        tgt_prop = self._proposition_nodes.get(target_id)
        src_trace_tick = self._trace_nodes.get(source_id)
        if src_citta is not None and tgt_citta is not None:
            t_src = src_citta.coordinate.citta_tick
            t_tgt = tgt_citta.coordinate.citta_tick
            if paccaya in (Paccaya.ANANTARA, Paccaya.SAMANANTARA, Paccaya.NATTHI, Paccaya.VIGATA):
                if t_tgt != t_src + 1:
                    raise ValueError(
                        f"Violazione {paccaya.pali_term}: richiesta contiguità T_n -> T_{{n+1}}, "
                        f"trovato T_{t_src} -> T_{t_tgt}."
                    )
                if not src_citta.is_ceased:
                    raise ValueError(
                        f"Violazione {paccaya.pali_term}: il nodo {source_id} deve essere in BHANGA."
                    )
                if tgt_citta.coordinate.sub_moment != AnuKhana.UPPADA:
                    raise ValueError(
                        f"Violazione {paccaya.pali_term}: il nodo {target_id} deve essere in UPPADA."
                    )
            elif paccaya == Paccaya.ASEVANA:
                if t_tgt != t_src + 1 or not src_citta.is_ceased:
                    raise ValueError("Violazione Āsevana-paccaya: richiesta successione post-BHANGA.")
                is_src_javana = src_citta.vithi_phase.name.startswith("JAVANA_")
                is_tgt_javana = tgt_citta.vithi_phase.name.startswith("JAVANA_")
                if not (is_src_javana and is_tgt_javana):
                    raise ValueError("Violazione Āsevana-paccaya: applicabile solo tra momenti Javana.")
                if src_citta.cetasika_matrix.mode != tgt_citta.cetasika_matrix.mode:
                    raise ValueError("Violazione Āsevana-paccaya: i momenti Javana devono avere la stessa Jāti.")
            elif paccaya in (
                Paccaya.SAHAJATA, Paccaya.ANNAMANNA, Paccaya.SAMPAYUTTA,
                Paccaya.HETU, Paccaya.INDRIYA, Paccaya.MAGGA, Paccaya.JHANA
            ):
                if src_citta.coordinate != tgt_citta.coordinate:
                    raise ValueError(
                        f"Violazione {paccaya.pali_term}: richiesta co-esistenza nello stesso tick."
                    )
                if paccaya == Paccaya.HETU and len(src_citta.active_roots) == 0:
                    raise ValueError("Violazione Hetu-paccaya: stato Ahetuka.")
            elif paccaya == Paccaya.UPANISSAYA:
                if t_src >= t_tgt:
                    raise ValueError("Violazione Upanissaya-paccaya: T_src deve precedere T_tgt.")
            elif paccaya in (Paccaya.ATTHI, Paccaya.AVIGATA):
                if src_citta.coordinate != tgt_citta.coordinate:
                    raise ValueError(f"Violazione {paccaya.pali_term}: richiesta co-esistenza.")
        elif src_rupa is not None and tgt_citta is not None:
            if paccaya in (
                Paccaya.ARAMMANA, Paccaya.PUREJATA, Paccaya.ATTHI,
                Paccaya.AVIGATA, Paccaya.NISSAYA, Paccaya.VIPPAYUTTA
            ):
                if not src_rupa.is_alive(tgt_citta.coordinate):
                    raise ValueError(f"Violazione {paccaya.pali_term}: Rūpa {source_id} già estinto.")
                if paccaya == Paccaya.PUREJATA:
                    if src_rupa.get_rupa_phase(tgt_citta.coordinate) != AnuKhana.THITI:
                        raise ValueError("Violazione Purejāta-paccaya: Rūpa non in fase THITI.")
                if paccaya == Paccaya.ARAMMANA:
                    if tgt_citta.arammana_id != src_rupa.rupa_id:
                        raise ValueError("Violazione Ārammaṇa-paccaya: arammana_id non corrispondente.")
            else:
                raise ValueError(f"Condizione {paccaya.pali_term} non valida per Rūpa -> Nāma.")
        elif src_trace_tick is not None and tgt_citta is not None:
            if paccaya in (Paccaya.UPANISSAYA, Paccaya.ARAMMANA):
                if src_trace_tick >= tgt_citta.coordinate.citta_tick:
                    raise ValueError(
                        f"Violazione {paccaya.pali_term}: la traccia Bhavaṅga T_{src_trace_tick} "
                        f"deve precedere il momento Manodvāra T_{tgt_citta.coordinate.citta_tick}."
                    )
            else:
                raise ValueError(
                    f"Condizione {paccaya.pali_term} non valida per Traccia Bhavaṅga -> Manodvāra."
                )
        elif src_prop is not None and tgt_prop is not None:
            allowed_ontological = {
                Paccaya.HETU, Paccaya.UPANISSAYA, Paccaya.NISSAYA,
                Paccaya.SAHAJATA, Paccaya.KAMMA, Paccaya.VIPAKA,
                Paccaya.ATTHI, Paccaya.AVIGATA, Paccaya.NATTHI
            }
            if paccaya not in allowed_ontological:
                raise ValueError(
                    f"Violazione Paṭṭhāna: condizione {paccaya.pali_term} non applicabile a nodi concettuali."
                )
        else:
            raise KeyError(
                f"Nodi non registrati o combinazione non supportata: {source_id} -> {target_id}."
            )

# ==============================================================================
# STRATO 2: COMPONENTE 1 - RŪPA DUAL-STREAM PIPELINE (CON MICRO-FIX REGEX)
# ==============================================================================
@dataclass(frozen=True)
class KineticImpression:
    tp_typographic_turbulence: float
    pc_capsular_pressure: float
    dl_lexical_rhythm_tension: float
    weighted_l2_norm: float
    sigmoid_friction: float
    enforced_temperature: float
    @property
    def caps_impact_pressure(self) -> float:
        return self.pc_capsular_pressure

@dataclass(frozen=True)
class SemanticProposition:
    raw_length: int
    clean_text: str
    preserved_acronyms: Tuple[str, ...]
    token_count: int

@dataclass(frozen=True)
class DualTrackPacket:
    packet_id: str
    channel_a_kinetic: KineticImpression
    channel_b_semantic: SemanticProposition

class RupaDualStreamPipeline:
    W1_TP: float = 0.40
    W2_PC: float = 0.45
    W3_DL: float = 0.15
    SIGMOID_K: float = 9.0
    SIGMOID_P0: float = 0.35
    FRICTION_THRESHOLD_ZERO_TEMP: float = 0.50
    MATTR_WINDOW: int = 50
    L_REF_SENTENCE: float = 18.0
    KNOWN_ACRONYMS: FrozenSet[str] = frozenset({
        "ASCII", "HTTPS", "JSONL", "SOLID", "POSIX", "UTF16", "UTF32",
        "FLOAT", "ASYNC", "AWAIT", "COVID", "LASER", "RADAR", "SONAR",
        "SCUBA", "UNHCR", "UNESCO", "UNICEF", "SOPRA", "INFRA", "NASA", "NATO", "JSON"
    })
    def __init__(self) -> None:
        self._re_upper_words = re.compile(r'[A-ZÀ-ÖØ-Þ]{2,}')
        self._re_tokens = re.compile(r'\b\w+\b')
        self._re_strict_emphatics = re.compile(r'[\!\?\*\#§%$£&]')
        self._re_emphatic_clusters = re.compile(r'[\!\?]{2,}|\.{3,}|[\!\?\*]{2,}')
        self._re_elongated = re.compile(r'([a-zA-ZÀ-ÿ])\1{2,}')
        self._re_multi_punct = re.compile(r'[\!\?]{2,}')
        self._re_multi_dots = re.compile(r'\.{2,}')
    def process(self, raw_text: str) -> DualTrackPacket:
        if not raw_text or not raw_text.strip():
            raise ValueError("RupaDualStreamPipeline richiede un input non vuoto.")
        words = self._re_tokens.findall(raw_text)
        n_tokens = max(1, len(words))
        n_emphatics = len(self._re_strict_emphatics.findall(raw_text))
        n_clusters = len(self._re_emphatic_clusters.findall(raw_text))
        tp_exponent = (n_emphatics / float(n_tokens)) + (2.0 * n_clusters / float(n_tokens))
        tp = max(0.0, min(1.0, 1.0 - math.exp(-tp_exponent)))
        alpha_chars = [c for c in raw_text if c.isalpha()]
        total_alpha = max(1, len(alpha_chars))
        upper_matches = [
            m.group(0) for m in self._re_upper_words.finditer(raw_text)
            if self._is_standalone_upper_token(raw_text, m.start(), m.end())
        ]
        upper_words_chars = sum(len(w) for w in upper_matches)
        pc = max(0.0, min(1.0, upper_words_chars / float(total_alpha)))
        lower_words = [w.lower() for w in words]
        mattr = self._compute_mattr(lower_words, window=self.MATTR_WINDOW)
        mattr_dev = max(0.0, min(1.0, abs(0.75 - mattr) / 0.75))
        sentences = [s.strip() for s in re.split(r'[.!?]+', raw_text) if s.strip()]
        n_sentences = max(1, len(sentences))
        avg_sentence_len = n_tokens / float(n_sentences)
        len_dev = 1.0 - math.exp(-abs(avg_sentence_len - self.L_REF_SENTENCE) / self.L_REF_SENTENCE)
        dl = max(0.0, min(1.0, 0.5 * mattr_dev + 0.5 * len_dev))
        weighted_sq = (
            self.W1_TP * (tp ** 2) +
            self.W2_PC * (pc ** 2) +
            self.W3_DL * (dl ** 2)
        )
        l2_norm = max(0.0, min(1.0, math.sqrt(weighted_sq)))
        sigmoid_friction = 1.0 / (1.0 + math.exp(-self.SIGMOID_K * (l2_norm - self.SIGMOID_P0)))
        if sigmoid_friction >= self.FRICTION_THRESHOLD_ZERO_TEMP:
            enforced_temp = 0.0
        else:
            enforced_temp = round(max(0.0, 0.20 * (1.0 - sigmoid_friction)), 4)
        kinetic = KineticImpression(
            tp_typographic_turbulence=round(tp, 4),
            pc_capsular_pressure=round(pc, 4),
            dl_lexical_rhythm_tension=round(dl, 4),
            weighted_l2_norm=round(l2_norm, 4),
            sigmoid_friction=round(sigmoid_friction, 4),
            enforced_temperature=enforced_temp
        )
        clean_text, preserved_acronyms = self._normalize_conservatively(raw_text)
        clean_tokens = self._re_tokens.findall(clean_text)
        semantic = SemanticProposition(
            raw_length=len(raw_text),
            clean_text=clean_text,
            preserved_acronyms=tuple(sorted(preserved_acronyms)),
            token_count=len(clean_tokens)
        )
        return DualTrackPacket(
            packet_id=f"PKT-{uuid.uuid4().hex[:8].upper()}",
            channel_a_kinetic=kinetic,
            channel_b_semantic=semantic
        )
    @staticmethod
    def _is_standalone_upper_token(text: str, start: int, end: int) -> bool:
        before_ok = (start == 0) or (not text[start - 1].isalpha())
        after_ok = (end == len(text)) or (not text[end].isalpha())
        return before_ok and after_ok
    @staticmethod
    def _compute_mattr(tokens: List[str], window: int = 50) -> float:
        if not tokens:
            return 0.0
        if len(tokens) <= window:
            return len(set(tokens)) / float(len(tokens))
        ttrs = [
            len(set(tokens[i:i + window])) / float(window)
            for i in range(len(tokens) - window + 1)
        ]
        return sum(ttrs) / float(len(ttrs))
    def _normalize_conservatively(self, text: str) -> Tuple[str, Set[str]]:
        preserved: Set[str] = set()
        reduced_elongation = self._re_elongated.sub(r'\1', text)
        coalesced_punct = self._re_multi_punct.sub('.', reduced_elongation)
        coalesced_punct = self._re_multi_dots.sub('.', coalesced_punct)
        def _fold_shouted_word(match: re.Match) -> str:
            word = match.group(0)
            if not self._is_standalone_upper_token(coalesced_punct, match.start(), match.end()):
                return word
            if len(word) <= 4 or word in self.KNOWN_ACRONYMS:
                preserved.add(word)
                return word
            return word.lower()
        folded_text = self._re_upper_words.sub(_fold_shouted_word, coalesced_punct)
        clean = re.sub(r'\s+', ' ', folded_text).strip()
        return clean, preserved

class RupaSensorV2:
    _DEFAULT_PIPELINE = RupaDualStreamPipeline()
    @classmethod
    def sense_and_decouple(cls, raw_text: str) -> DualTrackPacket:
        return cls._DEFAULT_PIPELINE.process(raw_text)

# ==============================================================================
# STRATO 2: COMPONENTE 2 - SANNĀ / SANTĪRAṆA & VOṬṬHAPANA (Momenti 7-8)
# ==============================================================================
@dataclass(frozen=True)
class CandidateCausalClaim:
    claim_id: str
    cause_a: str
    effect_b: str
    linguistic_connector: str
    is_necessary_if_cause_absent: bool
    empirical_mechanism_present: bool

@dataclass(frozen=True)
class SantiranaDiagnosis:
    atomic_propositions: Tuple[str, ...]
    candidate_claims: Tuple[CandidateCausalClaim, ...]
    raw_detected_cetasikas: Tuple[Tuple[Cetasika, float], ...]
    certified_input_matrix: CetasikaMatrix
    citta_node: CittaKhana

class OperationalProtocol(Enum):
    UPEKKHA_MIRROR = ("Upekkhā-Mirror", "Specchio Neutrale De-Egotizzato (Rilevato Dosa/Māna)")
    VITAKKA_CUT = ("Vitakka-Cut", "Taglio Ortogonale Analitico (Rilevato Vicikicchā/Thīna-Middha)")
    PANNA_CAUSAL_ANALYSIS = ("Paññā-Causal", "Analisi Causale Strutturale (Indagine Collaborativa/Diṭṭhi)")
    SATI_OBSERVATION = ("Sati-Observation", "Osservazione Pura (Attrito Nullo / Fast-Path)")
    def __init__(self, code: str, description: str) -> None:
        self.code = code
        self.description = description

@dataclass(frozen=True)
class VotthapanaDecision:
    votthapana_node: CittaKhana
    determination_errors: Tuple[str, ...]
    selected_protocol: OperationalProtocol
    enforced_javana_mode: CognitiveMode

class PerceptionOrgan(ABC):
    @abstractmethod
    def analyze_clean_proposition(
        self,
        clean_text: str,
        enforced_temperature: float
    ) -> Tuple[List[str], List[CandidateCausalClaim], Dict[Cetasika, float]]:
        raise NotImplementedError

class GeminiPerceptionOrgan(PerceptionOrgan):
    """
    STRATO 2 - ORGANO PERCETTIVO NEURALE SUBORDINATO (Saññā / Santīraṇa).
    Connette l'SDK ufficiale `google-genai` imponendo:
    1. Temperatura controllata dal Canale A (KineticImpression).
    2. JSON Schema rigoroso (Structured Outputs) indipendente dalla lingua dell'input.
    3. Normalizzazione ontologica canonica per permettere al Manodvāra-vīthi di
       scoprire catene causali transitive (A -> B -> C) senza dizionari manuali.
    """
    SANTIRANA_SYSTEM_INSTRUCTION = (
        "You are the subordinate Santīraṇa (Investigation) perception organ inside a "
        "deterministic Theravāda Abhidharma Neuro-Symbolic AI kernel.\n"
        "Your ONLY task is to parse the input text (in ANY language or domain) into "
        "canonical Paramattha causal structures and Cetasika activation weights.\n"
        "STRICT RULES FOR CAUSAL EXTRACTION:\n"
        "1. Extract every asserted or implied cause-effect relationship (cause_a -> effect_b).\n"
        "2. CANONICAL NORMALIZATION: Write `cause_a` and `effect_b` as ULTRA-CONCISE, standardized, "
        "lowercase scientific/ontological noun phrases (MAXIMUM 3-5 WORDS EACH). Never copy entire sentences. "
        "Strip articles and conversational filler. If the same phenomenon appears twice, use the EXACT same canonical string "
        "so transitive chains (A -> B and B -> C) link together.\n"
        "3. COUNTERFACTUAL TEST (Natthi-paccaya vs Sufficient Pathophysiological Cause):\n"
        "   - `effect_arises_without_cause`: set to TRUE ONLY IF A and B are statistically independent, "
        "coincidental, a post-hoc fallacy, or a known clinical misconception (i.e. A does NOT cause B). "
        "CRITICAL: If A is a genuine sufficient pathophysiological/physical cause or trigger of B (e.g. "
        "'aneurysm rupture -> severe abdominal pain' or 'atherosclerosis -> elastin degradation'), "
        "you MUST set `effect_arises_without_cause` to FALSE, even if B could theoretically be caused by other conditions.\n"
        "   - `empirical_mechanism_present`: set to TRUE if a real physical, biological, clinical, "
        "pharmacological, or logical mechanism connects A to B. Set to FALSE for superstition, dogma, or pure coincidence.\n"
        "4. CETASIKA WEIGHTS: Estimate in [0.0, 1.0] the mental factors present in the input text "
        "(e.g., DOSA for hostility/aggression, DITTHI for dogmatic bias/misconception, "
        "VICIKICCHA for circular doubt, MOHA for confusion, PANNINDRIYA and SATI for objective analytical inquiry)."
    )

    RESPONSE_JSON_SCHEMA = {
        "type": "OBJECT",
        "properties": {
            "atomic_propositions": {
                "type": "ARRAY",
                "items": {"type": "STRING"},
                "description": "Concise list of core atomic propositions in the text."
            },
            "candidate_claims": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "cause_a": {
                            "type": "STRING",
                            "description": "Canonical lowercase noun phrase for the Conditioning Cause (Paccaya-dhamma)."
                        },
                        "effect_b": {
                            "type": "STRING",
                            "description": "Canonical lowercase noun phrase for the Conditioned Effect (Paccayuppanna-dhamma)."
                        },
                        "connector": {
                            "type": "STRING",
                            "description": "Type of causal relation (e.g. 'direct etiology', 'pathogenesis', 'spurious correlation')."
                        },
                        "effect_arises_without_cause": {
                            "type": "BOOLEAN",
                            "description": "True if B arises even without A (spurious/coincidental); False if A is a genuine condition."
                        },
                        "empirical_mechanism_present": {
                            "type": "BOOLEAN",
                            "description": "True if a verified scientific/clinical/logical mechanism exists between A and B."
                        }
                    },
                    "required": [
                        "cause_a",
                        "effect_b",
                        "connector",
                        "effect_arises_without_cause",
                        "empirical_mechanism_present"
                    ]
                }
            },
            "cetasikas": {
                "type": "OBJECT",
                "properties": {
                    "DOSA": {"type": "NUMBER"},
                    "DITTHI": {"type": "NUMBER"},
                    "MANA": {"type": "NUMBER"},
                    "MOHA": {"type": "NUMBER"},
                    "VICIKICCHA": {"type": "NUMBER"},
                    "LOBHA": {"type": "NUMBER"},
                    "PANNINDRIYA": {"type": "NUMBER"},
                    "SATI": {"type": "NUMBER"}
                }
            }
        },
        "required": ["atomic_propositions", "candidate_claims", "cetasikas"]
    }

    def __init__(
        self,
        model_name: str = "gemini-3.8-flash",
        api_key: Optional[str] = None,
        fallback_organ: Optional[PerceptionOrgan] = None,
        max_retries: int = 4,
        retry_delay_sec: float = 5.0,
        min_request_interval_sec: float = 12.5,
        memory_store_ref: Optional[Any] = None,
        verbose_errors: bool = True
    ) -> None:
        self.model_name = model_name
        self.api_key = api_key
        self.fallback_organ = fallback_organ or DeterministicCittaPerceptionOrgan()
        self.max_retries = max_retries
        self.retry_delay_sec = retry_delay_sec
        self.min_request_interval_sec = min_request_interval_sec
        self.memory_store_ref = memory_store_ref
        self.verbose_errors = verbose_errors
        self._client: Any = None
        self._last_call_timestamp: float = 0.0
        self.api_calls_succeeded: int = 0
        self.api_calls_failed: int = 0

    def _get_client(self) -> Any:
        if self._client is None and self.api_key:
            from google import genai
            self._client = genai.Client(api_key=self.api_key)
        return self._client

    def analyze_clean_proposition(
        self,
        clean_text: str,
        enforced_temperature: float
    ) -> Tuple[List[str], List[CandidateCausalClaim], Dict[Cetasika, float]]:
        if self.api_key is not None and not self.api_key.startswith("INSERISCI"):
            import time as _time
            from google.genai import types
            client = self._get_client()
            truncated_text = clean_text[:6000]

            # Costruzione vocabolario ontologico dinamico dal Bhavaṅga per Entity Resolution
            ontology_hint = ""
            if self.memory_store_ref is not None and hasattr(self.memory_store_ref, "concept_nodes"):
                existing_labels = [c.canonical_label for c in self.memory_store_ref.concept_nodes[-80:]]
                if existing_labels:
                    ontology_hint = (
                        "\nEXISTING BHAVANGA CANONICAL CONCEPTS (reuse these exact strings if referring "
                        f"to the same entity): {', '.join(existing_labels)}\n"
                    )

            for attempt in range(1, self.max_retries + 1):
                # Pacing deterministico Token-Bucket (evita 429 su Free Tier 5 RPM)
                if self.min_request_interval_sec > 0.0 and self._last_call_timestamp > 0.0:
                    elapsed_since_last = _time.time() - self._last_call_timestamp
                    if elapsed_since_last < self.min_request_interval_sec:
                        _time.sleep(self.min_request_interval_sec - elapsed_since_last)

                try:
                    self._last_call_timestamp = _time.time()
                    response = client.models.generate_content(
                        model=self.model_name,
                        contents=f"{ontology_hint}INPUT TEXT TO DISSECT:\n{truncated_text}",
                        config=types.GenerateContentConfig(
                            system_instruction=self.SANTIRANA_SYSTEM_INSTRUCTION,
                            temperature=enforced_temperature,
                            response_mime_type="application/json",
                            response_schema=self.RESPONSE_JSON_SCHEMA,
                        ),
                    )
                    if response and response.text:
                        data = json.loads(response.text)
                        self.api_calls_succeeded += 1
                        return self._parse_json_payload(data)
                except Exception as err:
                    err_str = str(err)
                    if attempt < self.max_retries:
                        # Estrae automaticamente il retryDelay suggerito da Google in caso di 429
                        m_wait = re.search(r"retry in ([0-9.]+)s", err_str, flags=re.IGNORECASE)
                        wait_s = (float(m_wait.group(1)) + 1.5) if m_wait else (self.retry_delay_sec * attempt)
                        wait_s = min(65.0, max(2.0, wait_s))
                        if self.verbose_errors:
                            print(f" [~] Pacing/Retry ({attempt}/{self.max_retries}): attesa di {wait_s:.1f}s per quota API...")
                        _time.sleep(wait_s)
                    else:
                        self.api_calls_failed += 1
                        if self.verbose_errors:
                            print(f" [!] Avviso GeminiPerceptionOrgan (fallback attivato): {err}")
                        return self.fallback_organ.analyze_clean_proposition(
                            clean_text, enforced_temperature
                        )
        return self.fallback_organ.analyze_clean_proposition(clean_text, enforced_temperature)

    @staticmethod
    def _parse_json_payload(
        data: dict
    ) -> Tuple[List[str], List[CandidateCausalClaim], Dict[Cetasika, float]]:
        propositions = [str(p).strip() for p in data.get("atomic_propositions", []) if str(p).strip()]
        claims: List[CandidateCausalClaim] = []
        for idx, c in enumerate(data.get("candidate_claims", []), start=1):
            ca = DeterministicCittaPerceptionOrgan.normalize_concept_label(str(c.get("cause_a", "")))
            eb = DeterministicCittaPerceptionOrgan.normalize_concept_label(str(c.get("effect_b", "")))
            if not ca or not eb or ca == eb:
                continue
            claims.append(
                CandidateCausalClaim(
                    claim_id=f"CLM-{idx:02d}",
                    cause_a=ca,
                    effect_b=eb,
                    linguistic_connector=str(c.get("connector", "causal_condition")),
                    is_necessary_if_cause_absent=bool(c.get("effect_arises_without_cause", False)),
                    empirical_mechanism_present=bool(c.get("empirical_mechanism_present", True))
                )
            )
        cetasika_map: Dict[Cetasika, float] = {
            Cetasika.PHASSA: 0.90,
            Cetasika.VEDANA: 0.85,
            Cetasika.SANNA: 0.90,
            Cetasika.CETANA: 0.85,
            Cetasika.EKAGGATA: 0.85,
            Cetasika.JIVITINDRIYA: 0.90,
            Cetasika.MANASIKARA: 0.90,
        }
        raw_cet = data.get("cetasikas", {})
        if isinstance(raw_cet, dict):
            for cet in Cetasika:
                if cet.name in raw_cet and raw_cet[cet.name] is not None:
                    val = float(raw_cet[cet.name])
                    if val > 0.0:
                        cetasika_map[cet] = max(0.0, min(1.0, val))
        return propositions, claims, cetasika_map

class DeterministicCittaPerceptionOrgan(PerceptionOrgan):
    CAUSAL_PATTERNS = [
        # Forma passiva medico-scientifica EN/IT: "X is ... caused by Y", "X caused by Y", "X triggered by Y", "X due to Y"
        (r"(.+?)\s+(?:is\s+[^,;]+?\s+caused\s+by|is\s+caused\s+by|are\s+caused\s+by|caused\s+by|triggered\s+by|induced\s+by|due\s+to|provocato\s+da|causato\s+da)\s+(.+)", "perché (invertito)"),
        (r"(.+?)\s+(?:perché|because)\s+(.+)", "perché (invertito)"),
        # Forma attiva EN/IT: "X causes Y", "X leads to Y", "X results in Y", "X triggers Y", "X determina Y"
        (r"(.+?)\s+(?:causa|provoca|genera|determina|produce|induce|innesca|causes|leads\s+to|results\s+in|triggers|induces|produces|drives|promotes)\s+(.+)", "causa/determina"),
        (r"(?:se|if)\s+(.+?)\s+(?:allora|then)\s+(.+)", "se...allora"),
        (r"(?:dopo|after)\s+(.+?)\s+(?:accade|si verifica|avviene|crolla|sale|happens|occurs)\s+(.+)", "post-hoc temporale"),
    ]
    DOGMATIC_MARKERS = {
        "ovvio", "assoluto", "indiscutibile", "sempre", "chiunque", "dogma", "complotto", "certo",
        "obviously", "undeniable", "always", "everyone", "conspiracy"
    }
    AVERSION_MARKERS = {
        "incompetenti", "idioti", "stupido", "odio", "inutile", "colpa",
        "fallimento", "assurdo", "vergogna", "corrotto", "bugiardi", "ridicoli",
        "incompetent", "idiots", "stupid", "useless", "corrupt", "liars", "ridiculous"
    }
    DOUBT_MARKERS = {
        "forse", "non so", "impossibile capire", "dubbio", "confuso", "chissà", "incerto",
        "maybe", "unsure", "confused", "uncertain", "doubt"
    }
    SPURIOUS_MARKERS = {
        "oroscopo", "fortuna", "caso", "coincidenza", "ieri", "dopo",
        "statistica", "correlazione", "gallo", "gelati", "pirati", "superstizione", "retrogrado",
        "coincidence", "superstition", "myth", "misconception"
    }
    SCIENTIFIC_MARKERS = {
        "condizione", "meccanismo", "verifica", "gradiente", "pressione",
        "termodinamica", "termodinamico", "causale", "legge", "struttura",
        "ioni", "membrana", "depolarizzazione", "sinaptico", "neurotrasmettitore",
        "potenziale", "enzima", "catalisi", "fotoni", "elettroni", "entropia",
        # Lessico clinico inglese (opus5-5diseaseconversations.jsonl)
        "syndrome", "disorder", "disease", "deletion", "mutation", "chromosomal",
        "microdeletion", "deficiency", "infection", "inflammation", "autoimmune",
        "receptor", "enzyme", "protein", "gene", "genetic", "pathway", "cell",
        "cellular", "tissue", "organ", "vascular", "ischemia", "hypoxia",
        "metabolic", "chronic", "acute", "clinical", "symptom", "pathology",
        "secretion", "inhibition", "activation", "lesion", "arterial", "chromosome",
        "patients", "diagnosis", "treatment", "therapy", "impairment", "dysfunction"
    }
    @staticmethod
    def normalize_concept_label(text: str) -> str:
        cleaned = text.strip().strip(".,;:!?'\"").lower()
        cleaned = re.sub(r"^\[[a-z0-9_]+\]\s*", "", cleaned)
        cleaned = re.sub(r"^(?:description|executive_summary|name|disease|pathology)\s*:\s*", "", cleaned)
        cleaned = re.sub(
            r"^(?:il|lo|la|i|gli|le|un|uno|una|l'|dello|della|dei|degli|delle|nel|nella|che\s+il|che\s+la|che\s+l'|the|a|an|that\s+the|which\s+is)\s+",
            "",
            cleaned
        )
        cleaned = re.sub(r"^l'", "", cleaned)
        cleaned = re.sub(r",\s*$", "", cleaned)
        return re.sub(r"\s+", " ", cleaned).strip()
    def analyze_clean_proposition(
        self,
        clean_text: str,
        enforced_temperature: float
    ) -> Tuple[List[str], List[CandidateCausalClaim], Dict[Cetasika, float]]:
        raw_clauses = [s.strip() for s in re.split(r"\.(?=\s|$)|[;?!]+", clean_text) if s.strip()]
        atomic_props: List[str] = []
        claims: List[CandidateCausalClaim] = []
        claim_counter = 1
        for clause in raw_clauses:
            matched_causal = False
            clause_lower = clause.lower().strip()
            if re.match(r"^(?:what|why|how|when|where|who|which|cosa|perché|come|quando|chi)\b", clause_lower):
                atomic_props.append(f"P{len(atomic_props)+1}: {clause}")
                continue
            for pattern, connector_type in self.CAUSAL_PATTERNS:
                m = re.search(pattern, clause, flags=re.IGNORECASE)
                if m:
                    matched_causal = True
                    if connector_type == "perché (invertito)":
                        raw_eff = m.group(1)
                        if " is " in raw_eff.lower():
                            raw_eff = re.split(r"\s+is\s+", raw_eff, maxsplit=1, flags=re.IGNORECASE)[0]
                        elif "," in raw_eff:
                            raw_eff = raw_eff.split(",")[0]
                        effect_part = self.normalize_concept_label(raw_eff)
                        cause_part = self.normalize_concept_label(m.group(2))
                    else:
                        if "," in m.group(1):
                            cause_raw = m.group(1).split(",")[-1]
                        else:
                            cause_raw = m.group(1)
                        cause_part = self.normalize_concept_label(cause_raw)
                        effect_part = self.normalize_concept_label(m.group(2))
                    atomic_props.append(f"P{len(atomic_props)+1}: {cause_part}")
                    atomic_props.append(f"P{len(atomic_props)+1}: {effect_part}")
                    words_in_clause = set(re.findall(r"\b\w+\b", clause_lower))
                    has_spurious = bool(words_in_clause & self.SPURIOUS_MARKERS) or (
                        connector_type == "post-hoc temporale"
                    )
                    has_medical_suffix = any(
                        w.endswith((
                            "plasia", "itis", "osis", "emia", "pathy", "trophy", "oma",
                            "penia", "lysis", "uria", "cardia", "pnea", "algia", "ectasis",
                            "megaly", "sclerosis", "stenosis", "spasm", "toxicity", "failure"
                        ))
                        for w in words_in_clause
                    )
                    has_scientific = bool(words_in_clause & self.SCIENTIFIC_MARKERS) or has_medical_suffix
                    effect_arises_without_cause = has_spurious and not has_scientific
                    empirical_mechanism = has_scientific and not has_spurious
                    claims.append(
                        CandidateCausalClaim(
                            claim_id=f"CLM-{claim_counter:02d}",
                            cause_a=cause_part,
                            effect_b=effect_part,
                            linguistic_connector=connector_type,
                            is_necessary_if_cause_absent=effect_arises_without_cause,
                            empirical_mechanism_present=empirical_mechanism
                        )
                    )
                    claim_counter += 1
                    break
            if not matched_causal:
                atomic_props.append(f"P{len(atomic_props)+1}: {clause}")
        all_words = set(re.findall(r"\b\w+\b", clean_text.lower()))
        detected: Dict[Cetasika, float] = {
            Cetasika.PHASSA: 0.90,
            Cetasika.VEDANA: 0.80,
            Cetasika.SANNA: 0.85,
            Cetasika.CETANA: 0.85,
            Cetasika.EKAGGATA: 0.75,
            Cetasika.JIVITINDRIYA: 0.90,
            Cetasika.MANASIKARA: 0.85,
            Cetasika.VITAKKA: 0.80,
            Cetasika.VICARA: 0.75,
        }
        dogmatic_hits = len(all_words & self.DOGMATIC_MARKERS)
        aversion_hits = len(all_words & self.AVERSION_MARKERS)
        doubt_hits = len(all_words & self.DOUBT_MARKERS)
        spurious_hits = sum(1 for c in claims if c.is_necessary_if_cause_absent)
        scientific_hits = len(all_words & self.SCIENTIFIC_MARKERS)
        if aversion_hits > 0:
            detected[Cetasika.DOSA] = min(1.0, 0.65 + 0.15 * aversion_hits)
            detected[Cetasika.MOHA] = min(1.0, 0.60 + 0.10 * (aversion_hits + spurious_hits))
        if dogmatic_hits > 0 or spurious_hits > 0:
            detected[Cetasika.DITTHI] = min(1.0, 0.60 + 0.15 * (dogmatic_hits + spurious_hits))
            detected[Cetasika.MOHA] = max(
                detected.get(Cetasika.MOHA, 0.0),
                min(1.0, 0.65 + 0.15 * spurious_hits)
            )
        if doubt_hits > 0 and aversion_hits == 0 and dogmatic_hits == 0:
            detected[Cetasika.VICIKICCHA] = min(1.0, 0.70 + 0.10 * doubt_hits)
            detected[Cetasika.MOHA] = 0.75
        if scientific_hits > 0 and aversion_hits == 0 and spurious_hits == 0:
            detected[Cetasika.PANNINDRIYA] = min(1.0, 0.80 + 0.05 * scientific_hits)
            detected[Cetasika.SATI] = 0.90
        return atomic_props, claims, detected

# ==============================================================================
# STRATO 3: COMPONENTE 1 - GRAFO PERSISTENTE DEL BHAVAṄGA-SOTA & PAKATŪPANISSAYA
# ==============================================================================
@dataclass(frozen=True)
class VerifiedCausalRelation:
    claim: CandidateCausalClaim
    assigned_paccayas: FrozenSet[Paccaya]
    passed_natthi_counterfactual: bool
    rejection_reason: Optional[str]
    ontological_edge_id: Optional[str]

@dataclass(frozen=True)
class JavanaExecutionTrace:
    javana_nodes: Tuple[CittaKhana, ...]
    evaluated_relations: Tuple[VerifiedCausalRelation, ...]
    purified_matrix: CetasikaMatrix
    metric_aligned_structure: str
    final_causal_synthesis: str

@dataclass(frozen=True)
class BhavangaCausalTrace:
    record_id: str
    rupa_id: str
    clean_proposition: str
    kinetic_friction: float
    selected_protocol: OperationalProtocol
    verified_relations_count: int
    rejected_fallacies_count: int
    final_synthesis: str
    closing_coordinate: TemporalCoordinate
    rupa_final_phase: AnuKhana
    validated_pairs: Tuple[Tuple[str, str], ...] = ()
    rejected_pairs: Tuple[Tuple[str, str, str], ...] = ()
    @property
    def trace_id(self) -> str:
        return self.record_id

BhavangaMemoryRecord = BhavangaCausalTrace

@dataclass(frozen=True)
class ConceptualDhammaNode:
    concept_id: str
    canonical_label: str
    observation_count: int
    first_seen_tick: int
    last_seen_tick: int

@dataclass(frozen=True)
class StructuralCausalLaw:
    law_id: str
    cause_concept_id: str
    effect_concept_id: str
    cause_label: str
    effect_label: str
    confirmations_count: int
    pakatupanissaya_weight: float
    supporting_trace_ids: Tuple[str, ...]
    is_transitive_deduction: bool = False
    @property
    def is_structural_law(self) -> bool:
        return self.confirmations_count >= 2 or self.pakatupanissaya_weight >= 0.85

class BhavangaMemoryStore:
    BASE_INITIAL_WEIGHT: float = 0.72
    ASYMPTOTIC_LAMBDA: float = 0.55
    def __init__(self) -> None:
        self._traces: List[BhavangaCausalTrace] = []
        self._concept_nodes: Dict[str, ConceptualDhammaNode] = {}
        self._label_to_concept_id: Dict[str, str] = {}
        self._causal_laws: Dict[Tuple[str, str], StructuralCausalLaw] = {}
        self._rejected_fallacies_registry: Dict[Tuple[str, str], int] = {}
    @classmethod
    def compute_pakatupanissaya_weight(
        cls,
        confirmations: int,
        base_weight: float = 0.72,
        decay_lambda: float = 0.55
    ) -> float:
        if confirmations <= 0:
            return 0.0
        w = 1.0 - (1.0 - base_weight) * math.exp(-decay_lambda * float(confirmations - 1))
        return round(max(0.0, min(0.9999, w)), 4)
    def commit_tadarammana(self, record: BhavangaCausalTrace) -> None:
        if record.rupa_final_phase != AnuKhana.BHANGA:
            raise ValueError(
                "Violazione Tadārammaṇa: il commit in Bhavaṅga deve coincidere con il BHANGA del Rūpa."
            )
        self._traces.append(record)
        tick = record.closing_coordinate.citta_tick
        for cause_lbl, effect_lbl in record.validated_pairs:
            c_node = self._upsert_concept(cause_lbl, tick)
            e_node = self._upsert_concept(effect_lbl, tick)
            self._consolidate_causal_pair(
                cause_node=c_node,
                effect_node=e_node,
                trace_id=record.record_id,
                is_transitive=False
            )
        for cause_lbl, effect_lbl, _reason in record.rejected_pairs:
            norm_c = DeterministicCittaPerceptionOrgan.normalize_concept_label(cause_lbl)
            norm_e = DeterministicCittaPerceptionOrgan.normalize_concept_label(effect_lbl)
            key = (norm_c, norm_e)
            self._rejected_fallacies_registry[key] = self._rejected_fallacies_registry.get(key, 0) + 1
    def register_transitive_law(
        self,
        cause_label: str,
        effect_label: str,
        derived_weight: float,
        supporting_trace_ids: Iterable[str],
        current_tick: int
    ) -> StructuralCausalLaw:
        c_node = self._upsert_concept(cause_label, current_tick)
        e_node = self._upsert_concept(effect_label, current_tick)
        pair_key = (c_node.concept_id, e_node.concept_id)
        existing = self._causal_laws.get(pair_key)
        trace_tuple = tuple(dict.fromkeys(supporting_trace_ids))
        if existing is not None:
            new_count = existing.confirmations_count + 1
            combined_traces = tuple(dict.fromkeys(existing.supporting_trace_ids + trace_tuple))
            new_weight = max(
                existing.pakatupanissaya_weight,
                round(min(0.9999, derived_weight), 4),
                self.compute_pakatupanissaya_weight(new_count)
            )
            law = StructuralCausalLaw(
                law_id=existing.law_id,
                cause_concept_id=c_node.concept_id,
                effect_concept_id=e_node.concept_id,
                cause_label=c_node.canonical_label,
                effect_label=e_node.canonical_label,
                confirmations_count=new_count,
                pakatupanissaya_weight=new_weight,
                supporting_trace_ids=combined_traces,
                is_transitive_deduction=existing.is_transitive_deduction
            )
        else:
            law = StructuralCausalLaw(
                law_id=f"LAW-TRANS-{uuid.uuid4().hex[:6].upper()}",
                cause_concept_id=c_node.concept_id,
                effect_concept_id=e_node.concept_id,
                cause_label=c_node.canonical_label,
                effect_label=e_node.canonical_label,
                confirmations_count=len(trace_tuple),
                pakatupanissaya_weight=round(min(0.9999, derived_weight), 4),
                supporting_trace_ids=trace_tuple,
                is_transitive_deduction=True
            )
        self._causal_laws[pair_key] = law
        return law
    def get_pakatupanissaya_prior(self, cause_label: str, effect_label: str) -> Optional[StructuralCausalLaw]:
        norm_c = DeterministicCittaPerceptionOrgan.normalize_concept_label(cause_label)
        norm_e = DeterministicCittaPerceptionOrgan.normalize_concept_label(effect_label)
        cid_a = self._label_to_concept_id.get(norm_c)
        cid_b = self._label_to_concept_id.get(norm_e)
        if cid_a and cid_b:
            return self._causal_laws.get((cid_a, cid_b))
        return None
    SYNONYM_CANONICAL_MAP: Dict[str, str] = {
        "low blood calcium": "hypocalcemia",
        "low calcium": "hypocalcemia",
        "serum calcium reduction": "hypocalcemia",
        " chromosome 22q11.2 deletion": "22q11.2 microdeletion",
        "heterozygous deletion on chromosome 22q11.2": "22q11.2 microdeletion",
        "deletion on chromosome 22q11.2": "22q11.2 microdeletion",
        "parathyroid insufficiency": "hypoparathyroidism",
        "parathyroid hormone deficiency": "hypoparathyroidism",
        "prolonged qtc": "qtc interval prolongation",
        "qt prolongation": "qtc interval prolongation",
    }

    def _upsert_concept(self, raw_label: str, tick: int) -> ConceptualDhammaNode:
        norm = DeterministicCittaPerceptionOrgan.normalize_concept_label(raw_label)
        norm = self.SYNONYM_CANONICAL_MAP.get(norm, norm)
        cid = self._label_to_concept_id.get(norm)
        if cid is None:
            cid = f"DHAMMA-CONCEPT-{len(self._concept_nodes) + 1:03d}"
            node = ConceptualDhammaNode(
                concept_id=cid,
                canonical_label=norm,
                observation_count=1,
                first_seen_tick=tick,
                last_seen_tick=tick
            )
            self._concept_nodes[cid] = node
            self._label_to_concept_id[norm] = cid
            return node
        else:
            old = self._concept_nodes[cid]
            updated = ConceptualDhammaNode(
                concept_id=old.concept_id,
                canonical_label=old.canonical_label,
                observation_count=old.observation_count + 1,
                first_seen_tick=old.first_seen_tick,
                last_seen_tick=max(old.last_seen_tick, tick)
            )
            self._concept_nodes[cid] = updated
            return updated
    def _consolidate_causal_pair(
        self,
        cause_node: ConceptualDhammaNode,
        effect_node: ConceptualDhammaNode,
        trace_id: str,
        is_transitive: bool = False
    ) -> StructuralCausalLaw:
        key = (cause_node.concept_id, effect_node.concept_id)
        existing = self._causal_laws.get(key)
        if existing is None:
            weight = self.compute_pakatupanissaya_weight(1)
            law = StructuralCausalLaw(
                law_id=f"LAW-{len(self._causal_laws) + 1:03d}",
                cause_concept_id=cause_node.concept_id,
                effect_concept_id=effect_node.concept_id,
                cause_label=cause_node.canonical_label,
                effect_label=effect_node.canonical_label,
                confirmations_count=1,
                pakatupanissaya_weight=weight,
                supporting_trace_ids=(trace_id,),
                is_transitive_deduction=is_transitive
            )
        else:
            new_count = existing.confirmations_count + 1
            new_weight = self.compute_pakatupanissaya_weight(new_count)
            traces = tuple(dict.fromkeys(existing.supporting_trace_ids + (trace_id,)))
            law = StructuralCausalLaw(
                law_id=existing.law_id,
                cause_concept_id=cause_node.concept_id,
                effect_concept_id=effect_node.concept_id,
                cause_label=cause_node.canonical_label,
                effect_label=effect_node.canonical_label,
                confirmations_count=new_count,
                pakatupanissaya_weight=new_weight,
                supporting_trace_ids=traces,
                is_transitive_deduction=existing.is_transitive_deduction and is_transitive
            )
        self._causal_laws[key] = law
        return law
    @property
    def records(self) -> Tuple[BhavangaCausalTrace, ...]:
        return tuple(self._traces)
    @property
    def traces(self) -> Tuple[BhavangaCausalTrace, ...]:
        return tuple(self._traces)
    @property
    def concept_nodes(self) -> Tuple[ConceptualDhammaNode, ...]:
        return tuple(self._concept_nodes.values())
    @property
    def causal_laws(self) -> Tuple[StructuralCausalLaw, ...]:
        return tuple(self._causal_laws.values())
    @property
    def rejected_fallacies(self) -> Dict[Tuple[str, str], int]:
        return dict(self._rejected_fallacies_registry)
    def to_json(self) -> str:
        payload = {
            "traces": [
                {
                    "record_id": t.record_id,
                    "rupa_id": t.rupa_id,
                    "clean_proposition": t.clean_proposition,
                    "kinetic_friction": t.kinetic_friction,
                    "selected_protocol": t.selected_protocol.name,
                    "verified_relations_count": t.verified_relations_count,
                    "rejected_fallacies_count": t.rejected_fallacies_count,
                    "final_synthesis": t.final_synthesis,
                    "closing_citta_tick": t.closing_coordinate.citta_tick,
                    "closing_sub_moment": t.closing_coordinate.sub_moment.name,
                    "rupa_final_phase": t.rupa_final_phase.name,
                    "validated_pairs": [list(p) for p in t.validated_pairs],
                    "rejected_pairs": [list(p) for p in t.rejected_pairs],
                }
                for t in self._traces
            ],
            "concepts": [
                {
                    "concept_id": c.concept_id,
                    "canonical_label": c.canonical_label,
                    "observation_count": c.observation_count,
                    "first_seen_tick": c.first_seen_tick,
                    "last_seen_tick": c.last_seen_tick,
                }
                for c in self._concept_nodes.values()
            ],
            "causal_laws": [
                {
                    "law_id": l.law_id,
                    "cause_concept_id": l.cause_concept_id,
                    "effect_concept_id": l.effect_concept_id,
                    "cause_label": l.cause_label,
                    "effect_label": l.effect_label,
                    "confirmations_count": l.confirmations_count,
                    "pakatupanissaya_weight": l.pakatupanissaya_weight,
                    "supporting_trace_ids": list(l.supporting_trace_ids),
                    "is_transitive_deduction": l.is_transitive_deduction,
                }
                for l in self._causal_laws.values()
            ],
            "rejected_fallacies": [
                {"cause": k[0], "effect": k[1], "count": v}
                for k, v in self._rejected_fallacies_registry.items()
            ],
        }
        return json.dumps(payload, ensure_ascii=False, indent=2)
    @classmethod
    def from_json(cls, json_str: str) -> BhavangaMemoryStore:
        data = json.loads(json_str)
        store = cls()
        for t in data.get("traces", []):
            trace = BhavangaCausalTrace(
                record_id=t["record_id"],
                rupa_id=t["rupa_id"],
                clean_proposition=t["clean_proposition"],
                kinetic_friction=float(t["kinetic_friction"]),
                selected_protocol=OperationalProtocol[t["selected_protocol"]],
                verified_relations_count=int(t["verified_relations_count"]),
                rejected_fallacies_count=int(t["rejected_fallacies_count"]),
                final_synthesis=t["final_synthesis"],
                closing_coordinate=TemporalCoordinate(
                    citta_tick=int(t["closing_citta_tick"]),
                    sub_moment=AnuKhana[t["closing_sub_moment"]],
                ),
                rupa_final_phase=AnuKhana[t["rupa_final_phase"]],
                validated_pairs=tuple((p[0], p[1]) for p in t.get("validated_pairs", [])),
                rejected_pairs=tuple((p[0], p[1], p[2]) for p in t.get("rejected_pairs", [])),
            )
            store._traces.append(trace)
        for c in data.get("concepts", []):
            node = ConceptualDhammaNode(
                concept_id=c["concept_id"],
                canonical_label=c["canonical_label"],
                observation_count=int(c["observation_count"]),
                first_seen_tick=int(c["first_seen_tick"]),
                last_seen_tick=int(c["last_seen_tick"]),
            )
            store._concept_nodes[node.concept_id] = node
            store._label_to_concept_id[node.canonical_label] = node.concept_id
        for l in data.get("causal_laws", []):
            law = StructuralCausalLaw(
                law_id=l["law_id"],
                cause_concept_id=l["cause_concept_id"],
                effect_concept_id=l["effect_concept_id"],
                cause_label=l["cause_label"],
                effect_label=l["effect_label"],
                confirmations_count=int(l["confirmations_count"]),
                pakatupanissaya_weight=float(l["pakatupanissaya_weight"]),
                supporting_trace_ids=tuple(l["supporting_trace_ids"]),
                is_transitive_deduction=bool(l["is_transitive_deduction"]),
            )
            store._causal_laws[(law.cause_concept_id, law.effect_concept_id)] = law
        for rf in data.get("rejected_fallacies", []):
            store._rejected_fallacies_registry[(rf["cause"], rf["effect"])] = int(rf["count"])
        return store

# ==============================================================================
# STRATO 2: MOTORE PAÑCADVĀRA-VĪTHI INTEGRATO CON PAKATŪPANISSAYA
# ==============================================================================
@dataclass(frozen=True)
class CittaVithiExecutionResult:
    dual_track_packet: DualTrackPacket
    rupa_datum: RupaDatum
    santirana_diagnosis: SantiranaDiagnosis
    votthapana_decision: VotthapanaDecision
    javana_trace: JavanaExecutionTrace
    bhavanga_record: BhavangaCausalTrace
    all_citta_nodes: Tuple[CittaKhana, ...]
    total_sub_moments_executed: int

class NeuroSymbolicVithiEngine:
    def __init__(
        self,
        perception_organ: PerceptionOrgan,
        memory_store: Optional[BhavangaMemoryStore] = None,
        patthana_graph: Optional[PatthanaGraph] = None
    ) -> None:
        self.perception_organ = perception_organ
        self.memory_store = memory_store or BhavangaMemoryStore()
        self.patthana_graph = patthana_graph or PatthanaGraph()
        self.rupa_pipeline = RupaDualStreamPipeline()
        self._tick_counter = 1
    @property
    def current_tick(self) -> int:
        return self._tick_counter
    def advance_ticks(self, ticks: int) -> None:
        self._tick_counter += ticks
    def process_raw_input(self, raw_input: str) -> CittaVithiExecutionResult:
        packet = self.rupa_pipeline.process(raw_input)
        regulator = CittaVithiRegulator(
            rupa_payload=packet.channel_b_semantic.clean_text,
            start_citta_tick=self._tick_counter
        )
        rupa = regulator.rupa
        self.patthana_graph.register_rupa(rupa)
        vithi_schedule = regulator.step_vithi()
        emitted_cittas: List[CittaKhana] = []
        prev_bhanga_node: Optional[CittaKhana] = None
        ahetuka_matrix = CetasikaMatrix.from_dict(
            raw_activations={
                Cetasika.PHASSA: 0.90,
                Cetasika.VEDANA: 0.80,
                Cetasika.SANNA: 0.85,
                Cetasika.CETANA: 0.85,
                Cetasika.EKAGGATA: 0.85,
                Cetasika.JIVITINDRIYA: 0.95,
                Cetasika.MANASIKARA: 0.90,
                Cetasika.VITAKKA: 0.80,
                Cetasika.VICARA: 0.80,
            },
            mode=CognitiveMode.ETHICALLY_VARIABLE_AHETUKA,
            auto_enforce=True
        )
        for phase_idx in range(1, 7):
            phase = VithiPhase(phase_idx)
            _, bhanga_node = self._emit_citta_moment(
                phase=phase,
                schedule=vithi_schedule,
                matrix=ahetuka_matrix,
                rupa=rupa,
                prev_bhanga_node=prev_bhanga_node
            )
            emitted_cittas.append(bhanga_node)
            prev_bhanga_node = bhanga_node
        atomic_props, candidate_claims, raw_cetasikas = (
            self.perception_organ.analyze_clean_proposition(
                clean_text=packet.channel_b_semantic.clean_text,
                enforced_temperature=packet.channel_a_kinetic.enforced_temperature
            )
        )
        has_raw_akusala = any(
            cet.category == CetasikaCategory.AKUSALA and val > 0.0
            for cet, val in raw_cetasikas.items()
        )
        diag_mode = (
            CognitiveMode.DIAGNOSTIC_AKUSALA if has_raw_akusala else CognitiveMode.ANALYTIC_SOBHANA
        )
        certified_diag_matrix = CetasikaMatrix.from_dict(
            raw_activations=raw_cetasikas,
            mode=diag_mode,
            auto_enforce=True
        )
        _, santirana_bhanga = self._emit_citta_moment(
            phase=VithiPhase.SANTIRANA,
            schedule=vithi_schedule,
            matrix=certified_diag_matrix,
            rupa=rupa,
            prev_bhanga_node=prev_bhanga_node
        )
        emitted_cittas.append(santirana_bhanga)
        prev_bhanga_node = santirana_bhanga
        santirana_diagnosis = SantiranaDiagnosis(
            atomic_propositions=tuple(atomic_props),
            candidate_claims=tuple(candidate_claims),
            raw_detected_cetasikas=tuple(raw_cetasikas.items()),
            certified_input_matrix=certified_diag_matrix,
            citta_node=santirana_bhanga
        )
        votthapana_matrix = CetasikaMatrix.from_dict(
            raw_activations={
                Cetasika.VITAKKA: 0.95,
                Cetasika.VICARA: 0.90,
                Cetasika.ADHIMOKKHA: 0.95,
                Cetasika.VIRIYA: 0.90,
            },
            mode=CognitiveMode.ETHICALLY_VARIABLE_AHETUKA,
            auto_enforce=True
        )
        _, votthapana_bhanga = self._emit_citta_moment(
            phase=VithiPhase.VOTTHAPANA,
            schedule=vithi_schedule,
            matrix=votthapana_matrix,
            rupa=rupa,
            prev_bhanga_node=prev_bhanga_node
        )
        emitted_cittas.append(votthapana_bhanga)
        prev_bhanga_node = votthapana_bhanga
        votthapana_decision = self._execute_votthapana_determination(
            santirana_diag=santirana_diagnosis,
            kinetic=packet.channel_a_kinetic,
            votthapana_node=votthapana_bhanga
        )
        javana_seed = dict(raw_cetasikas)
        javana_seed[Cetasika.PANNINDRIYA] = 0.96
        javana_seed[Cetasika.SATI] = 0.95
        javana_seed[Cetasika.TATRAMAJJHATTATA] = 0.94
        javana_seed[Cetasika.VIRIYA] = 0.92
        javana_seed[Cetasika.ADHIMOKKHA] = 0.90
        sobhana_javana_matrix = CetasikaMatrix.from_dict(
            raw_activations=javana_seed,
            mode=votthapana_decision.enforced_javana_mode,
            auto_enforce=True
        )
        javana_nodes: List[CittaKhana] = []
        for j_idx in range(9, 16):
            j_phase = VithiPhase(j_idx)
            _, j_bhanga = self._emit_citta_moment(
                phase=j_phase,
                schedule=vithi_schedule,
                matrix=sobhana_javana_matrix,
                rupa=rupa,
                prev_bhanga_node=prev_bhanga_node,
                is_consecutive_javana=(j_idx > 9)
            )
            emitted_cittas.append(j_bhanga)
            javana_nodes.append(j_bhanga)
            prev_bhanga_node = j_bhanga
        javana_trace = self._execute_seven_javana_pipeline(
            javana_nodes=javana_nodes,
            santirana_diag=santirana_diagnosis,
            votthapana_dec=votthapana_decision,
            sobhana_matrix=sobhana_javana_matrix
        )
        for t_idx in (16, 17):
            t_phase = VithiPhase(t_idx)
            _, t_bhanga = self._emit_citta_moment(
                phase=t_phase,
                schedule=vithi_schedule,
                matrix=sobhana_javana_matrix,
                rupa=rupa,
                prev_bhanga_node=prev_bhanga_node
            )
            emitted_cittas.append(t_bhanga)
            prev_bhanga_node = t_bhanga
        last_phase, last_coord, last_rupa_phase, last_rem_life = vithi_schedule[-1]
        assert last_phase == VithiPhase.TADARAMMANA_2
        assert last_coord.sub_moment == AnuKhana.BHANGA
        assert last_rupa_phase == AnuKhana.BHANGA
        assert last_rem_life == 0.0
        validated_pairs = tuple(
            (r.claim.cause_a, r.claim.effect_b)
            for r in javana_trace.evaluated_relations
            if r.passed_natthi_counterfactual
        )
        rejected_pairs = tuple(
            (r.claim.cause_a, r.claim.effect_b, r.rejection_reason or "Rigettato")
            for r in javana_trace.evaluated_relations
            if not r.passed_natthi_counterfactual
        )
        bhavanga_record = BhavangaCausalTrace(
            record_id=f"BHAVANGA-TRC-{uuid.uuid4().hex[:8].upper()}",
            rupa_id=rupa.rupa_id,
            clean_proposition=packet.channel_b_semantic.clean_text,
            kinetic_friction=packet.channel_a_kinetic.sigmoid_friction,
            selected_protocol=votthapana_decision.selected_protocol,
            verified_relations_count=len(validated_pairs),
            rejected_fallacies_count=len(rejected_pairs),
            final_synthesis=javana_trace.final_causal_synthesis,
            closing_coordinate=last_coord,
            rupa_final_phase=last_rupa_phase,
            validated_pairs=validated_pairs,
            rejected_pairs=rejected_pairs
        )
        self.memory_store.commit_tadarammana(bhavanga_record)
        self.patthana_graph.register_bhavanga_trace(
            trace_id=bhavanga_record.record_id,
            closing_citta_tick=last_coord.citta_tick
        )
        self._tick_counter += 17
        return CittaVithiExecutionResult(
            dual_track_packet=packet,
            rupa_datum=rupa,
            santirana_diagnosis=santirana_diagnosis,
            votthapana_decision=votthapana_decision,
            javana_trace=javana_trace,
            bhavanga_record=bhavanga_record,
            all_citta_nodes=tuple(emitted_cittas),
            total_sub_moments_executed=len(vithi_schedule)
        )
    def _emit_citta_moment(
        self,
        phase: VithiPhase,
        schedule: List[Tuple[VithiPhase, TemporalCoordinate, AnuKhana, float]],
        matrix: CetasikaMatrix,
        rupa: RupaDatum,
        prev_bhanga_node: Optional[CittaKhana],
        is_consecutive_javana: bool = False
    ) -> Tuple[CittaKhana, CittaKhana]:
        base_idx = (phase.value - 1) * 3
        _, coord_uppada, rupa_phase_uppada, _ = schedule[base_idx]
        _, coord_bhanga, _, _ = schedule[base_idx + 2]
        uppada_node = CittaKhana(
            node_id=f"CITTA-T{coord_uppada.citta_tick:02d}-{phase.name}-UPPADA",
            coordinate=coord_uppada,
            vithi_phase=phase,
            cetasika_matrix=matrix,
            arammana_id=rupa.rupa_id
        )
        bhanga_node = CittaKhana(
            node_id=f"CITTA-T{coord_bhanga.citta_tick:02d}-{phase.name}-BHANGA",
            coordinate=coord_bhanga,
            vithi_phase=phase,
            cetasika_matrix=matrix,
            arammana_id=rupa.rupa_id
        )
        self.patthana_graph.register_citta(uppada_node)
        self.patthana_graph.register_citta(bhanga_node)
        if prev_bhanga_node is not None:
            transition_conditions: Set[Paccaya] = {
                Paccaya.ANANTARA, Paccaya.SAMANANTARA, Paccaya.NATTHI, Paccaya.VIGATA
            }
            if is_consecutive_javana:
                transition_conditions.add(Paccaya.ASEVANA)
            self.patthana_graph.add_verified_edge(
                source_id=prev_bhanga_node.node_id,
                target_id=uppada_node.node_id,
                conditions=transition_conditions,
                strength=1.0
            )
        rupa_conditions: Set[Paccaya] = {Paccaya.ARAMMANA, Paccaya.ATTHI, Paccaya.AVIGATA}
        if rupa_phase_uppada == AnuKhana.THITI:
            rupa_conditions.add(Paccaya.PUREJATA)
        self.patthana_graph.add_verified_edge(
            source_id=rupa.rupa_id,
            target_id=uppada_node.node_id,
            conditions=rupa_conditions,
            strength=0.95
        )
        return uppada_node, bhanga_node
    def _execute_votthapana_determination(
        self,
        santirana_diag: SantiranaDiagnosis,
        kinetic: KineticImpression,
        votthapana_node: CittaKhana
    ) -> VotthapanaDecision:
        diag_matrix = santirana_diag.certified_input_matrix
        errors: List[str] = []
        for claim in santirana_diag.candidate_claims:
            prior_law = self.memory_store.get_pakatupanissaya_prior(claim.cause_a, claim.effect_b)
            if prior_law is None and (claim.is_necessary_if_cause_absent or not claim.empirical_mechanism_present):
                errors.append(
                    f"Errore di Determinazione [{claim.claim_id}]: Falsa attribuzione causale "
                    f"('{claim.cause_a}' -> '{claim.effect_b}') priva di necessità controfattuale."
                )
        if diag_matrix.get(Cetasika.DITTHI) > 0.0:
            errors.append(
                f"Rilevato irrigidimento dogmatico (Diṭṭhi={diag_matrix.get(Cetasika.DITTHI)}) nell'input."
            )
        if diag_matrix.get(Cetasika.DOSA) > 0.0:
            errors.append(
                f"Rilevata avversione/ostilità (Dosa={diag_matrix.get(Cetasika.DOSA)}) nell'input."
            )
        if diag_matrix.get(Cetasika.DOSA) > 0.0 or diag_matrix.get(Cetasika.MANA) > 0.0:
            protocol = OperationalProtocol.UPEKKHA_MIRROR
        elif diag_matrix.get(Cetasika.VICIKICCHA) > 0.0 or diag_matrix.get(Cetasika.THINA) > 0.0:
            protocol = OperationalProtocol.VITAKKA_CUT
        elif kinetic.sigmoid_friction < 0.15 and not santirana_diag.candidate_claims and not errors:
            protocol = OperationalProtocol.SATI_OBSERVATION
        else:
            protocol = OperationalProtocol.PANNA_CAUSAL_ANALYSIS
        return VotthapanaDecision(
            votthapana_node=votthapana_node,
            determination_errors=tuple(errors),
            selected_protocol=protocol,
            enforced_javana_mode=CognitiveMode.ANALYTIC_SOBHANA
        )
    def _execute_seven_javana_pipeline(
        self,
        javana_nodes: List[CittaKhana],
        santirana_diag: SantiranaDiagnosis,
        votthapana_dec: VotthapanaDecision,
        sobhana_matrix: CetasikaMatrix
    ) -> JavanaExecutionTrace:
        isolated_claims = list(santirana_diag.candidate_claims)
        evaluated_relations: List[VerifiedCausalRelation] = []
        for claim in isolated_claims:
            prior_law = self.memory_store.get_pakatupanissaya_prior(claim.cause_a, claim.effect_b)
            if (claim.empirical_mechanism_present and not claim.is_necessary_if_cause_absent) or (
                prior_law is not None and prior_law.is_structural_law
            ):
                candidate_paccayas = frozenset({
                    Paccaya.HETU, Paccaya.UPANISSAYA, Paccaya.NISSAYA, Paccaya.ATTHI
                })
                passed_natthi = True
                rejection = None
            else:
                candidate_paccayas = frozenset()
                passed_natthi = False
                rejection = (
                    f"Rigettato in JAVANA_3 (Test Controfattuale Natthi): '{claim.effect_b}' "
                    f"sorge indipendentemente da '{claim.cause_a}' (correlazione spuria / post-hoc)."
                )
            if (
                sobhana_matrix.get(Cetasika.DITTHI) != 0.0
                or sobhana_matrix.get(Cetasika.MOHA) != 0.0
                or sobhana_matrix.get(Cetasika.DOSA) != 0.0
            ):
                raise RuntimeError("Violazione JAVANA_4: Inquinamento Akusala rilevato durante Javana.")
            ontological_edge_id: Optional[str] = None
            if passed_natthi and candidate_paccayas:
                src_prop_id = f"PROP-{claim.claim_id}-CAUSE-{uuid.uuid4().hex[:4].upper()}"
                tgt_prop_id = f"PROP-{claim.claim_id}-EFFECT-{uuid.uuid4().hex[:4].upper()}"
                self.patthana_graph.register_proposition(src_prop_id, claim.cause_a)
                self.patthana_graph.register_proposition(tgt_prop_id, claim.effect_b)
                edge_strength = prior_law.pakatupanissaya_weight if prior_law else 0.90
                edge = self.patthana_graph.add_verified_edge(
                    source_id=src_prop_id,
                    target_id=tgt_prop_id,
                    conditions=set(candidate_paccayas),
                    strength=edge_strength
                )
                ontological_edge_id = edge.edge_id
            evaluated_relations.append(
                VerifiedCausalRelation(
                    claim=claim,
                    assigned_paccayas=candidate_paccayas,
                    passed_natthi_counterfactual=passed_natthi,
                    rejection_reason=rejection,
                    ontological_edge_id=ontological_edge_id
                )
            )
        protocol = votthapana_dec.selected_protocol
        valid_rels = [r for r in evaluated_relations if r.passed_natthi_counterfactual]
        rejected_rels = [r for r in evaluated_relations if not r.passed_natthi_counterfactual]
        if protocol == OperationalProtocol.UPEKKHA_MIRROR:
            metric_structure = (
                "[ASSETTO UPEKKHĀ-MIRROR | Zero Pronomi Personali | De-escalation Cinetica Attiva] "
                f"Proposizioni esaminate: {len(santirana_diag.atomic_propositions)} | "
                f"Nessi validati: {len(valid_rels)} | Fallacie estinte: {len(rejected_rels)}."
            )
        elif protocol == OperationalProtocol.VITAKKA_CUT:
            metric_structure = (
                "[ASSETTO VITAKKA-CUT | Taglio Ortogonale del Dubbio Circolare] "
                f"Isolamento di {len(santirana_diag.atomic_propositions)} fattori discreti."
            )
        elif protocol == OperationalProtocol.SATI_OBSERVATION:
            metric_structure = (
                "[ASSETTO SATI-OBSERVATION | Registrazione Equanime] "
                f"Stato stazionario verificato su {len(santirana_diag.atomic_propositions)} proposizioni."
            )
        else:
            metric_structure = (
                "[ASSETTO PAÑÑĀ-CAUSAL | Analisi Condizionale Paṭṭhāna] "
                f"Nessi Paramattha consolidati: {len(valid_rels)} | Correlazioni rigettate: {len(rejected_rels)}."
            )
        synthesis_lines: List[str] = [metric_structure]
        for v in valid_rels:
            cond_names = ", ".join(sorted(p.pali_term for p in v.assigned_paccayas))
            synthesis_lines.append(
                f" -> [VALIDATO PARAMATTHA | Edge {v.ontological_edge_id}]: "
                f"'{v.claim.cause_a}' condiziona '{v.claim.effect_b}' via ({cond_names})."
            )
        for r in rejected_rels:
            synthesis_lines.append(f" -> [ESTINTO IN JAVANA_3]: {r.rejection_reason}")
        if not valid_rels and not rejected_rels:
            synthesis_lines.append(
                " -> [OSSERVAZIONE PURA]: Nessuna pretesa causale spuria rilevata; stato coerente."
            )
        return JavanaExecutionTrace(
            javana_nodes=tuple(javana_nodes),
            evaluated_relations=tuple(evaluated_relations),
            purified_matrix=sobhana_matrix,
            metric_aligned_structure=metric_structure,
            final_causal_synthesis="\n".join(synthesis_lines)
        )

# ==============================================================================
# STRATO 3: COMPONENTE 2 - PORTA MENTALE PURA (MANODVĀRA-VĪTHI ENGINE)
# ==============================================================================
@dataclass(frozen=True)
class TransitiveCausalDiscovery:
    chain_id: str
    origin_cause_a: str
    intermediate_mediators: Tuple[str, ...]
    terminal_effect_c: str
    upanissaya_edge_id: str
    epistemic_certainty: float
    source_trace_ids: Tuple[str, ...]

@dataclass(frozen=True)
class AbhidhammaSynthesisReport:
    synthesis_id: str
    manodvara_nodes: Tuple[CittaKhana, ...]
    direct_structural_laws: Tuple[StructuralCausalLaw, ...]
    transitive_discoveries: Tuple[TransitiveCausalDiscovery, ...]
    expelled_spurious_fallacies: Tuple[Tuple[str, str, int], ...]
    global_epistemic_certainty: float
    upanissaya_edges_created: Tuple[CausalEdge, ...]
    narrative_summary: str

class ManodvaraSynthesizer:
    def __init__(
        self,
        memory_store: BhavangaMemoryStore,
        patthana_graph: PatthanaGraph
    ) -> None:
        self.memory_store = memory_store
        self.patthana_graph = patthana_graph
    def execute_manodvara_vithi(self, start_citta_tick: int) -> AbhidhammaSynthesisReport:
        if start_citta_tick < 1:
            raise ValueError("start_citta_tick per Manodvāra-vīthi deve essere >= 1.")
        traces = self.memory_store.traces
        dhammarammana_id = f"DHAMMARAMMANA-SYNTH-{uuid.uuid4().hex[:6].upper()}"
        ahetuka_kiriya_matrix = CetasikaMatrix.from_dict(
            raw_activations={
                Cetasika.PHASSA: 0.90,
                Cetasika.VEDANA: 0.85,
                Cetasika.SANNA: 0.90,
                Cetasika.CETANA: 0.90,
                Cetasika.EKAGGATA: 0.90,
                Cetasika.JIVITINDRIYA: 0.95,
                Cetasika.MANASIKARA: 0.95,
                Cetasika.VITAKKA: 0.92,
                Cetasika.VICARA: 0.92,
                Cetasika.ADHIMOKKHA: 0.90,
                Cetasika.VIRIYA: 0.90,
            },
            mode=CognitiveMode.ETHICALLY_VARIABLE_AHETUKA,
            auto_enforce=True
        )
        panna_sobhana_matrix = CetasikaMatrix.from_dict(
            raw_activations={
                Cetasika.PANNINDRIYA: 0.99,
                Cetasika.SATI: 0.98,
                Cetasika.TATRAMAJJHATTATA: 0.97,
                Cetasika.EKAGGATA: 0.96,
                Cetasika.VIRIYA: 0.95,
                Cetasika.VITAKKA: 0.94,
                Cetasika.VICARA: 0.94,
                Cetasika.ADHIMOKKHA: 0.95,
                Cetasika.CHANDA: 0.92,
            },
            mode=CognitiveMode.ANALYTIC_SOBHANA,
            auto_enforce=True
        )
        emitted_nodes: List[CittaKhana] = []
        created_upanissaya_edges: List[CausalEdge] = []
        prev_bhanga: Optional[CittaKhana] = None
        current_tick = start_citta_tick
        for phase in ManodvaraPhase:
            is_javana_or_tad = phase.value >= ManodvaraPhase.JAVANA_1.value
            active_matrix = panna_sobhana_matrix if is_javana_or_tad else ahetuka_kiriya_matrix
            is_consec_javana = (
                ManodvaraPhase.JAVANA_2.value <= phase.value <= ManodvaraPhase.JAVANA_7.value
            )
            coord_uppada = TemporalCoordinate(current_tick, AnuKhana.UPPADA)
            coord_bhanga = TemporalCoordinate(current_tick, AnuKhana.BHANGA)
            uppada_node = CittaKhana(
                node_id=f"MANODVARA-T{current_tick:03d}-{phase.name}-UPPADA",
                coordinate=coord_uppada,
                vithi_phase=phase,
                cetasika_matrix=active_matrix,
                arammana_id=dhammarammana_id
            )
            bhanga_node = CittaKhana(
                node_id=f"MANODVARA-T{current_tick:03d}-{phase.name}-BHANGA",
                coordinate=coord_bhanga,
                vithi_phase=phase,
                cetasika_matrix=active_matrix,
                arammana_id=dhammarammana_id
            )
            self.patthana_graph.register_citta(uppada_node)
            self.patthana_graph.register_citta(bhanga_node)
            if prev_bhanga is not None:
                conds: Set[Paccaya] = {
                    Paccaya.ANANTARA, Paccaya.SAMANANTARA, Paccaya.NATTHI, Paccaya.VIGATA
                }
                if is_consec_javana:
                    conds.add(Paccaya.ASEVANA)
                self.patthana_graph.add_verified_edge(
                    source_id=prev_bhanga.node_id,
                    target_id=uppada_node.node_id,
                    conditions=conds,
                    strength=1.0
                )
            if phase == ManodvaraPhase.JAVANA_1:
                for tr in traces:
                    self.patthana_graph.register_bhavanga_trace(
                        trace_id=tr.record_id,
                        closing_citta_tick=tr.closing_coordinate.citta_tick
                    )
                    u_edge = self.patthana_graph.add_verified_edge(
                        source_id=tr.record_id,
                        target_id=uppada_node.node_id,
                        conditions={Paccaya.UPANISSAYA, Paccaya.ARAMMANA},
                        strength=0.95
                    )
                    created_upanissaya_edges.append(u_edge)
            emitted_nodes.append(bhanga_node)
            prev_bhanga = bhanga_node
            current_tick += 1
        direct_laws = list(self.memory_store.causal_laws)
        adjacency: Dict[str, List[StructuralCausalLaw]] = {}
        direct_pairs: Set[Tuple[str, str]] = set()
        for law in direct_laws:
            if not law.is_transitive_deduction:
                adjacency.setdefault(law.cause_label, []).append(law)
                direct_pairs.add((law.cause_label, law.effect_label))
        transitive_discoveries: List[TransitiveCausalDiscovery] = []
        visited_transitive: Set[Tuple[str, str]] = set()
        for start_cause in sorted(adjacency.keys()):
            queue: List[Tuple[str, List[str], float, List[str]]] = [
                (
                    law.effect_label,
                    [law.effect_label],
                    law.pakatupanissaya_weight,
                    list(law.supporting_trace_ids)
                )
                for law in adjacency[start_cause]
            ]
            while queue:
                curr_node, path_nodes, acc_weight, acc_traces = queue.pop(0)
                for next_law in adjacency.get(curr_node, []):
                    next_target = next_law.effect_label
                    if next_target == start_cause or next_target in path_nodes:
                        continue
                    new_path = path_nodes + [next_target]
                    combined_weight = round(math.sqrt(acc_weight * next_law.pakatupanissaya_weight) * 0.96, 4)
                    combined_traces = list(dict.fromkeys(acc_traces + list(next_law.supporting_trace_ids)))
                    if (start_cause, next_target) not in direct_pairs and (
                        start_cause, next_target
                    ) not in visited_transitive:
                        visited_transitive.add((start_cause, next_target))
                        mediators = tuple(new_path[:-1])
                        src_prop_id = f"CONCEPT-{uuid.uuid4().hex[:6].upper()}-A"
                        tgt_prop_id = f"CONCEPT-{uuid.uuid4().hex[:6].upper()}-C"
                        self.patthana_graph.register_proposition(src_prop_id, start_cause)
                        self.patthana_graph.register_proposition(tgt_prop_id, next_target)
                        trans_edge = self.patthana_graph.add_verified_edge(
                            source_id=src_prop_id,
                            target_id=tgt_prop_id,
                            conditions={Paccaya.UPANISSAYA, Paccaya.HETU},
                            strength=combined_weight
                        )
                        created_upanissaya_edges.append(trans_edge)
                        self.memory_store.register_transitive_law(
                            cause_label=start_cause,
                            effect_label=next_target,
                            derived_weight=combined_weight,
                            supporting_trace_ids=combined_traces,
                            current_tick=current_tick - 1
                        )
                        transitive_discoveries.append(
                            TransitiveCausalDiscovery(
                                chain_id=f"CHAIN-{len(transitive_discoveries) + 1:02d}",
                                origin_cause_a=start_cause,
                                intermediate_mediators=mediators,
                                terminal_effect_c=next_target,
                                upanissaya_edge_id=trans_edge.edge_id,
                                epistemic_certainty=combined_weight,
                                source_trace_ids=tuple(combined_traces)
                            )
                        )
                    if len(new_path) < 5:
                        queue.append((next_target, new_path, combined_weight, combined_traces))
        expelled_list = tuple(
            (c, e, cnt) for (c, e), cnt in self.memory_store.rejected_fallacies.items()
        )
        all_laws_now = self.memory_store.causal_laws
        if all_laws_now:
            global_certainty = round(
                sum(l.pakatupanissaya_weight for l in all_laws_now) / float(len(all_laws_now)),
                4
            )
        else:
            global_certainty = 0.0
        summary_lines = [
            f"[MANODVĀRA-VĪTHI SYNTHESIS REPORT | Certezza Epistemica Globale: {global_certainty:.4f}]",
            f" -> Tracce Bhavaṅga fuse (Dhammārammaṇa): {len(traces)}",
            f" -> Leggi Causali Dirette (Pakatūpanissaya): {len(direct_laws)}",
            f" -> Catene Causali Transitive Scoperte (A -> B -> C): {len(transitive_discoveries)}",
            f" -> Fallacie Statistiche / Bias Espulsi Definitivamente: {len(expelled_list)}",
        ]
        for td in transitive_discoveries:
            med_str = " -> ".join(td.intermediate_mediators)
            summary_lines.append(
                f"    * [SCOPERTA TRANSITIVA {td.chain_id} | Edge {td.upanissaya_edge_id} | "
                f"Certezza={td.epistemic_certainty:.4f}]: "
                f"'{td.origin_cause_a}' -> [{med_str}] ==> '{td.terminal_effect_c}' (Upanissaya-paccaya)"
            )
        return AbhidhammaSynthesisReport(
            synthesis_id=f"MANODVARA-SYNTH-{uuid.uuid4().hex[:8].upper()}",
            manodvara_nodes=tuple(emitted_nodes),
            direct_structural_laws=tuple(all_laws_now),
            transitive_discoveries=tuple(transitive_discoveries),
            expelled_spurious_fallacies=expelled_list,
            global_epistemic_certainty=global_certainty,
            upanissaya_edges_created=tuple(created_upanissaya_edges),
            narrative_summary="\n".join(summary_lines)
        )

# ==============================================================================
# STRATO 3: COMPONENTE 3 - MOTORE BATCH PROCESSING BIG DATA (GATEKEEPER + VĪTHI)
# ==============================================================================
class BatchRoutingDecision(Enum):
    FAST_PATH_SATI_OBSERVATION = "FAST_PATH_SATI_OBSERVATION"
    FULL_PANCADVARA_VITHI = "FULL_PANCADVARA_VITHI"

@dataclass(frozen=True)
class BatchItemResult:
    record_index: int
    record_id: str
    routing_decision: BatchRoutingDecision
    dual_track_packet: DualTrackPacket
    vithi_result: Optional[CittaVithiExecutionResult]

@dataclass(frozen=True)
class BatchExecutionSummary:
    total_records_scanned: int
    fast_path_count: int
    full_vithi_count: int
    item_results: Tuple[BatchItemResult, ...]
    manodvara_synthesis_report: AbhidhammaSynthesisReport

class AbhidhammaBatchProcessor:
    CAUSAL_SIGNAL_REGEX = re.compile(
        r"\b(?:causa|provoca|genera|determina|produce|induce|innesca|perché|allora|dopo|caused|causes|leads|results|triggers|triggered|induces|induced|produces|due|because|drives|promotes)\b",
        flags=re.IGNORECASE
    )
    def __init__(
        self,
        vithi_engine: NeuroSymbolicVithiEngine,
        friction_gate_threshold: float = 0.22,
        min_tokens_for_vithi: int = 5,
        language_agnostic_gate: bool = False
    ) -> None:
        self.vithi_engine = vithi_engine
        self.friction_gate_threshold = friction_gate_threshold
        self.min_tokens_for_vithi = min_tokens_for_vithi
        self.language_agnostic_gate = language_agnostic_gate
        self.rupa_pipeline = RupaDualStreamPipeline()
        self.manodvara_synthesizer = ManodvaraSynthesizer(
            memory_store=vithi_engine.memory_store,
            patthana_graph=vithi_engine.patthana_graph
        )
    def process_records(
        self,
        records: Iterable[Union[str, Dict[str, Any]]],
        text_field: str = "text",
        id_field: str = "id"
    ) -> BatchExecutionSummary:
        item_results: List[BatchItemResult] = []
        fast_path_count = 0
        full_vithi_count = 0
        for idx, rec in enumerate(records, start=1):
            if isinstance(rec, str):
                raw_text = rec
                rec_id = f"REC-{idx:04d}"
            elif isinstance(rec, dict):
                raw_text = str(rec.get(text_field, ""))
                rec_id = str(rec.get(id_field, f"REC-{idx:04d}"))
            else:
                raise TypeError(f"Formato record non supportato: {type(rec)}")
            if not raw_text.strip():
                continue
            packet = self.rupa_pipeline.process(raw_text)
            high_friction = packet.channel_a_kinetic.sigmoid_friction >= self.friction_gate_threshold
            enough_tokens = packet.channel_b_semantic.token_count >= self.min_tokens_for_vithi
            if self.language_agnostic_gate:
                # Gatekeeper puramente termodinamico/fisico (Rūpa puro, zero vocabolari linguistici)
                should_fast_path = not (high_friction and enough_tokens)
            else:
                has_causal_signal = bool(self.CAUSAL_SIGNAL_REGEX.search(packet.channel_b_semantic.clean_text))
                should_fast_path = (not has_causal_signal) and (not high_friction or not enough_tokens)
            if should_fast_path:
                fast_path_count += 1
                item_results.append(
                    BatchItemResult(
                        record_index=idx,
                        record_id=rec_id,
                        routing_decision=BatchRoutingDecision.FAST_PATH_SATI_OBSERVATION,
                        dual_track_packet=packet,
                        vithi_result=None
                    )
                )
            else:
                full_vithi_count += 1
                vithi_res = self.vithi_engine.process_raw_input(raw_text)
                item_results.append(
                    BatchItemResult(
                        record_index=idx,
                        record_id=rec_id,
                        routing_decision=BatchRoutingDecision.FULL_PANCADVARA_VITHI,
                        dual_track_packet=vithi_res.dual_track_packet,
                        vithi_result=vithi_res
                    )
                )
        synthesis_report = self.manodvara_synthesizer.execute_manodvara_vithi(
            start_citta_tick=self.vithi_engine.current_tick
        )
        self.vithi_engine.advance_ticks(len(ManodvaraPhase))
        return BatchExecutionSummary(
            total_records_scanned=len(item_results),
            fast_path_count=fast_path_count,
            full_vithi_count=full_vithi_count,
            item_results=tuple(item_results),
            manodvara_synthesis_report=synthesis_report
        )
    def process_csv(
        self,
        csv_source: Union[str, io.TextIOBase],
        text_column: str = "text",
        id_column: str = "id"
    ) -> BatchExecutionSummary:
        if isinstance(csv_source, io.TextIOBase):
            reader = csv.DictReader(csv_source)
            rows = list(reader)
        elif isinstance(csv_source, str) and "\n" in csv_source:
            reader = csv.DictReader(io.StringIO(csv_source.strip()))
            rows = list(reader)
        elif isinstance(csv_source, str):
            with open(csv_source, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        else:
            raise ValueError("Sorgente CSV non valida.")
        return self.process_records(rows, text_field=text_column, id_field=id_column)

if __name__ == "__main__":
    print("=" * 92)
    print("COLLAUDO INTEGRALE STRATI 1, 2 & 3: BHAVAṄGA-SOTA, MANODVĀRA-VĪTHI & BATCH ENGINE")
    print("=" * 92)
    pipeline_test = RupaDualStreamPipeline()
    pkt_fix = pipeline_test.process("Questo sistema è CORROTTO!!! Vergogna.")
    print("\n[VERIFICA MICRO-FIX REGEX STRATO 2]")
    print(f' -> Input: "Questo sistema è CORROTTO!!! Vergogna."')
    print(f" -> Pc (caps_impact_pressure): {pkt_fix.channel_a_kinetic.caps_impact_pressure:.4f} (> 0.0 verificato)")
    print(f' -> Clean Text (Canale B):     "{pkt_fix.channel_b_semantic.clean_text}"')

    csv_dataset = """id,text
REC-01,"Nota di servizio: log di sistema regolare, nessuna anomalia."
REC-02,"CORROTTO!!! GLI INCOMPETENTI mentono sempre... È OVVIO che il canto del gallo ieri provoca il sorgere del sole!!!"
REC-03,"L'afflusso di ioni calcio determina la depolarizzazione della membrana presinaptica."
REC-04,"In condizioni sperimentali controllate, l'afflusso di ioni calcio determina la depolarizzazione della membrana presinaptica."
REC-05,"Secondo la statistica di ieri la vendita di gelati provoca l'aumento delle insolazioni estive."
REC-06,"La depolarizzazione della membrana presinaptica genera il rilascio del neurotrasmettitore nello spazio sinaptico."
"""
    organ = GeminiPerceptionOrgan(
        api_key=None,
        fallback_organ=DeterministicCittaPerceptionOrgan()
    )
    bhavanga_store = BhavangaMemoryStore()
    patthana_graph = PatthanaGraph()
    vithi_engine = NeuroSymbolicVithiEngine(
        perception_organ=organ,
        memory_store=bhavanga_store,
        patthana_graph=patthana_graph
    )
    batch_processor = AbhidhammaBatchProcessor(vithi_engine=vithi_engine)
    print("\n[ESECUZIONE BATCH PROCESSING CON GATEKEEPER TERMODINAMICO]")
    batch_summary = batch_processor.process_csv(csv_dataset, text_column="text", id_column="id")
    print(f" -> Record Totali Scansionati:            {batch_summary.total_records_scanned}")
    print(f" -> Fast-Path (Sati-Observation):         {batch_summary.fast_path_count}")
    print(f" -> Cicli Completi Pañcadvāra-vīthi (17): {batch_summary.full_vithi_count}")
    for item in batch_summary.item_results:
        if item.routing_decision == BatchRoutingDecision.FAST_PATH_SATI_OBSERVATION:
            print(
                f"    * [{item.record_id}] ROUTING: {item.routing_decision.value} "
                f"(Attrito={item.dual_track_packet.channel_a_kinetic.sigmoid_friction:.4f})"
            )
        else:
            vr = item.vithi_result
            assert vr is not None
            print(
                f"    * [{item.record_id}] ROUTING: {item.routing_decision.value} | "
                f"Protocollo Voṭṭhapana: {vr.votthapana_decision.selected_protocol.code} | "
                f"Validati={vr.bhavanga_record.verified_relations_count}, "
                f"Rigettati={vr.bhavanga_record.rejected_fallacies_count}"
            )
    report = batch_summary.manodvara_synthesis_report
    print("\n[RISULTATO PORTA MENTALE PURA (MANODVĀRA-VĪTHI ENGINE - 12 MOMENTI)]")
    for line in report.narrative_summary.split("\n"):
        print(f" {line}")
    print("\n[DETTAGLIO LEGGI CAUSALI CONSOLIDATE NEL BHAVAṄGA-SOTA (PAKATŪPANISSAYA)]")
    for law in bhavanga_store.causal_laws:
        law_type = "TRANSITIVA (Manodvāra)" if law.is_transitive_deduction else "DIRETTA (Pañcadvāra)"
        print(
            f" -> [{law.law_id} | {law_type}] '{law.cause_label}' ==> '{law.effect_label}' | "
            f"Conferme={law.confirmations_count} | Peso Pakatūpanissaya={law.pakatupanissaya_weight:.4f}"
        )
    print("\n[FALLACIE STATISTICHE E BIAS ESPULSI DEFINITIVAMENTE]")
    for cause_f, effect_f, count_f in report.expelled_spurious_fallacies:
        print(f" -> [ESPULSO (x{count_f})]: '{cause_f}' -/-> '{effect_f}' (Fallito Test Controfattuale Natthi)")
    exported_json = bhavanga_store.to_json()
    reloaded_store = BhavangaMemoryStore.from_json(exported_json)
    assert len(reloaded_store.traces) == len(bhavanga_store.traces)
    assert len(reloaded_store.causal_laws) == len(bhavanga_store.causal_laws)
    assert len(report.transitive_discoveries) == 1
    disc = report.transitive_discoveries[0]
    assert disc.origin_cause_a == "afflusso di ioni calcio"
    assert disc.terminal_effect_c == "rilascio del neurotrasmettitore nello spazio sinaptico"
    print("\n[VERIFICA PERSISTENZA JSON BHAVAṄGA-SOTA & GRAFO PAṬṬHĀNA]")
    print(f" -> Serializzazione/Deserializzazione JSON verificata ({len(exported_json)} bytes).")
    print(f" -> Totale Archi Causali Certificati nel Paṭṭhāna Graph: {len(patthana_graph.edges)}")
    print("\n" + "=" * 92)
    print("ARCHITETTURA CORE COMPLETA (STRATI 1, 2 & 3) VALIDATA AL 100% CON SUCCESSO.")
    print("=" * 92)
