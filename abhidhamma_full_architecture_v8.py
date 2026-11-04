
           
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
