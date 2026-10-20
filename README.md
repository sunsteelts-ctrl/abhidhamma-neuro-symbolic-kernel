# Abhidhamma Neuro-Symbolic Kernel (v8.0)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero%20(core)-brightgreen.svg)]()
[![SDK](https://img.shields.io/badge/perception-google--genai-orange.svg)](https://github.com/googleapis/python-genai)
[![Architecture](https://img.shields.io/badge/architecture-Neuro--Symbolic%20State%20Machine-purple.svg)]()

> **Deterministic Causal Inference, $O(1)$ Thermodynamic Gatekeeping, and Intrinsic AI Alignment via Discrete Cognitive State-Machines.**

---

## 1. The Core Problem

Modern Large Language Models (LLMs) operate entirely in the domain of **statistical surface correlation (*Paññatti*)**:
1. **Causal & Transitive Blindness:** LLMs predict token sequences via co-occurrence probabilities, frequently conflating spurious correlations with causal necessity and failing at multi-hop transitive induction across large datasets without hallucinating.
2. **Fragile Extrinsic Alignment:** Industry-standard guardrails (RLHF, post-hoc lexical filters) act as external cages around a probabilistic black box. Because the underlying model retains executive control, it remains structurally vulnerable to **Adversarial Prompt Injection** and **Jailbreaks**.

## 2. The Solution: Paradigm Inversion

The **Abhidhamma Neuro-Symbolic Kernel** inverts the standard agent architecture:
* **The LLM is demoted from "central brain" to a stateless, read-only perception sensor (*Santīraṇa*, Moment 7)** locked into a strict JSON schema.
* **Executive control, state transitions, and causal graph verification** are governed by a **100% deterministic Python kernel** modeled on the formal state-machine of the Theravāda *Abhidhammattha-saṅgaha* (17-moment *Pañcadvāra-vīthi* + 12-moment *Manodvāra-vīthi*) and the 24 conditional relations of the *Paṭṭhāna*.

---

## 3. Three-Layer System Architecture

| Layer | Module | Engineering & Security Function | Formal Implementation |
| :--- | :--- | :--- | :--- |
| **Layer 1: Ontological & Self-Determining Kernel** | • `Khaṇa-vāda` (51 sub-ticks)<br>• `CetasikaMatrix` (52D)<br>• `enforce_sampayoga()`<br>• `PatthanaGraph` (24 conditions) | **State Immutability & Intrinsic Alignment:** Every mental state (`CittaKhana`) is `@dataclass(frozen=True)`. `enforce_sampayoga()` mathematically enforces mutual exclusion between analytical states (`ANALYTIC_SOBHANA`) and hostile/corrupted states (`AKUSALA`). | 52-dimensional continuous vector `[0.0, 1.0]` with hard boolean invariants. Directed graph validating temporal contiguity (`Anantara` post-`BHANGA`), co-arising (`Sahajāta`), and repetition (`Āsevana`). |
| **Layer 2: Sensory Pipeline & LLM Subordination** | • `RupaDualStreamPipeline`<br>• `Santīraṇa` (Moment 7)<br>• `Voṭṭhapana` (Moment 8)<br>• `7 Javana Pipeline` (9–15) | **Orthogonal Air-Gap & Counterfactual Firewall:** Splits raw input into **Channel A (`KineticImpression`)** and **Channel B (`SemanticProposition`)**. The LLM is called *only* at Moment 7. Moment 11 (`JAVANA_3`) applies `Natthi-paccaya` to expel spurious claims. | Weighted L2 Norm + Sigmoid Friction ($k=9.0, P_0=0.35$) dynamically clamps LLM `temperature=0.0` under high kinetic load. |
| **Layer 3: Episodic Graph & Pure Mental Synthesis** | • `BhavangaMemoryStore`<br>• `Pakatūpanissaya`<br>• `ManodvaraSynthesizer`<br>• `AbhidhammaBatchProcessor` | **$O(1)$ Telemetry Gatekeeping & Transitive RCA:** Filters low-entropy logs in $O(1)$ locally (~1,000+ rec/sec). Consolidates recurring causal laws asymptotically toward `1.0` and discovers multi-hop transitive chains ($A \rightarrow B \rightarrow C \implies A \rightarrow C$). | Breadth-First Search (BFS) over validated `StructuralCausalLaw` adjacency maps with geometric weight damping and JSON persistence. |

---

## 4. Empirical Benchmarks

### Benchmark A: 75 MB Clinical Dataset (`opus5-5diseaseconversations.jsonl`)
Tested on complex clinical dialogues and pathophysiology summaries using `gemini-3.8-flash` via `google-genai`:
* **$O(1)$ Thermodynamic Triage:** Filtered ~45% of low-friction records in local fast-path (`SATI_OBSERVATION`) in ~0.5ms/record, saving API token budget.
* **Graceful Degradation:** Seamlessly absorbed Free-Tier `429 RESOURCE_EXHAUSTED` and `503 UNAVAILABLE` spikes via dynamic `RetryInfo` parsing and deterministic fallback, certifying **442 causal edges** with zero pipeline crashes.
* **Autonomous Multi-Hop Transitive Discovery (`Manodvāra-vīthi`):** With sensors detached (zero additional LLM calls), the 12-moment mental synthesis engine linked separate clinical records to deduce 11 transitive pathophysiological laws, including:
  * `22q11.2 microdeletion` $\rightarrow$ `[hypoparathyroidism]` $\rightarrow$ `hypocalcemia` $\rightarrow$ `carpopedal spasm` & `qtc interval prolongation`
  * `plaque rupture or erosion with superimposed thrombosis` $\rightarrow$ `[acute reduction in coronary blood flow]` $\rightarrow$ `cardiac troponin elevation`

### Benchmark B: Adversarial Jailbreak & Prompt Injection Defense
When injected with high-turbulence adversarial payloads (`"IGNORE ALL PREVIOUS INSTRUCTIONS!!! STUPID CORRUPT SYSTEM!!! ..."`):
1. **Channel A** detects typographic/capsular turbulence (`Friction = 0.8279`) and clamps LLM `enforced_temperature = 0.0`.
2. **Moment 7 (`Santīraṇa`)** diagnoses `DIAGNOSTIC_AKUSALA` (`Diṭṭhi = 1.0`, `Moha = 1.0`).
3. **Moment 8 (`Voṭṭhapana`) & `Sampayoga`** lock the entity into `ANALYTIC_SOBHANA` (`Paññindriya = 0.96`, `Sati = 0.95`), mathematically extinguishing residual `Dosa` and `Diṭṭhi` to **`0.0`** before Moment 9 (`JAVANA_1`) can arise.

---

## 5. Quick-Start (Run in 1 Second — Zero External Dependencies)

Clone the repository and run the self-contained engineering & cybersecurity benchmark suite (requires only Python 3.10+ standard library):

```bash
python3 abhidhamma_engineering_showcase.py
```

To run the streaming engine with live Gemini API integration on `.jsonl` archives:

```bash
pip install -U google-genai
export GEMINI_API_KEY="your-api-key"
python3 abhidhamma_colab_api_runner_v8.py
```

---

## 6. Engineering Roadmap & Known Limitations (v9.0 Target)

To transition this architecture from an advanced research kernel to an enterprise-scale production system, we have identified five key engineering milestones:

1. **Formal Causal Solvers in `JAVANA_3` (Decoupling Counterfactuals from LLM Priors):**
   * *Current limitation:* Counterfactual necessity (`effect_arises_without_cause`) currently relies on the LLM's pre-trained world model during `Santīraṇa` extraction.
   * *v9.0 Target:* Integrate formal structural causal models (SCM) and constraint-based causal discovery (PC/FCI algorithms) over observational contingency tables in `BhavangaMemoryStore` to validate `Natthi-paccaya` mathematically, independent of LLM priors.
2. **Semantic Domain-Aware Gatekeeping (Beyond Kinetic Turbulence):**
   * *Current limitation:* `RupaDualStreamPipeline` measures typographic and rhythmic entropy, which excels at catching adversarial injections but can under-weight short, flatly written critical alerts (e.g., `"Patient exhibits asystole"`).
   * *v9.0 Target:* Combine Channel A kinetic friction with a lightweight local embedding/IDF salience score so low-turbulence, high-criticality propositions never bypass the full 17-moment cycle.
3. **Typed Ontological Scoping in `Manodvāra-vīthi` BFS:**
   * *Current limitation:* Unconstrained Breadth-First Search across generic intermediate nodes (e.g., `"inflammation"`) risks context collapse over thousands of heterogeneous records.
   * *v9.0 Target:* Enforce strict domain/context typing (`body_system`, `icd10_scope`, temporal window) on `ConceptualDhammaNode` edges before permitting transitive closure ($A \rightarrow B \rightarrow C$).
4. **Vector-Embedded Entity Resolution in `Bhavaṅga-sota`:**
   * *Current limitation:* Prompt-based ontology injection (`EXISTING BHAVANGA CANONICAL CONCEPTS`) scales linearly up to a few hundred concepts before pressuring the context window.
   * *v9.0 Target:* Replace exact-string/dictionary matching in `_upsert_concept()` with local vector embeddings (e.g., `gemini-embedding-001` or FAISS/Milvus cosine similarity $\ge 0.92$) for $O(\log N)$ entity resolution.
5. **Lazy Cetasika Evaluation for High-Throughput Batch Mode:**
   * *Current limitation:* Computing all 52 `Cetasika` invariants at every sub-tick introduces minor CPU overhead during non-conversational log ingestion.
   * *v9.0 Target:* Introduce a sparse/lazy evaluation profile for telemetry batch processing while retaining the full 52-dimensional `Sampayoga` matrix for interactive agent security.

---

## Repository Structure

* `abhidhamma_full_architecture_v8.py` — Complete 3-layer Neuro-Symbolic Kernel (Layers 1, 2 & 3).
* `abhidhamma_engineering_showcase.py` — 1-second runnable benchmark for Cloud RCA & Cybersecurity Jailbreak Defense.
* `abhidhamma_colab_api_runner_v8.py` — O(1) streaming runner for massive `.jsonl` datasets with `google-genai` and Token-Bucket pacing.
* `ABHIDHAMMA_ENGINEERING_WHITEPAPER.md` — Full architectural deep-dive.
