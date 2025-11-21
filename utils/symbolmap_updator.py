import asyncio
import numpy as np
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import normalize
from rapidfuzz.fuzz import token_sort_ratio
from core.base import Base

class AsyncSymbolMapUpdater(Base):
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        embedding_weight: float = 0.65,
        fuzzy_weight: float = 0.35,
        threshold: float = 0.80
    ):
        super().__init__("Symbolmap_embadding_match")
        self.model = SentenceTransformer(model_name)
        self.embedding_weight = embedding_weight
        self.fuzzy_weight = fuzzy_weight
        self.threshold = threshold        
        self.logger.info("AsyncSymbolMapUpdater initialized with model %s", model_name)

    async def _load_embeddings_async(self):
        self.logger.info("Loading symbol map embeddings from database...")
        data = self.collection_symbolmap_embeddings.find({})
        company_symbol_embeddings = []
        symbol_only_embeddings = []
        symbol_entries = []
        nse_to_entry = {}
        async for entry in data:
            company_symbol_embeddings.append(entry["embedding_company_and_symbol"])
            symbol_only_embeddings.append(entry["embedding_symbol_only"])
            symbol_entries.append(entry["symbol_map"])
            nse_to_entry[entry["symbol_map"]["NSE"]] = entry["symbol_map"]
        self.embedding_company_stocks = normalize(np.array(company_symbol_embeddings))
        self.embedding_stocks = normalize(np.array(symbol_only_embeddings))
        self.symbol_entries = symbol_entries
        self.nse_to_entry = nse_to_entry
        self.logger.info("Loaded %d symbol map embeddings.", len(symbol_entries))

    def load_embeddings(self):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            self.logger.info("Event loop is running, scheduling async embedding load...")
            loop.create_task(self._load_embeddings_async())
        else:
            self.logger.info("Event loop not running, loading embeddings synchronously...")
            loop.run_until_complete(self._load_embeddings_async())

    def get_best_match(
        self,
        query: str,
        combinedtype: str,
        stock: Optional[str] = None,
        threshold: Optional[float] = None
    ) -> Tuple[Optional[Dict], float, float, float]:
        if threshold is None:
            threshold = self.threshold

        if stock and stock in self.nse_to_entry:
            self.logger.info("Direct NSE lookup: '%s' found.", stock)
            return self.nse_to_entry[stock], 1.0, 1.0, 1.0

        if not query or not query.strip():
            self.logger.warning("Empty or invalid query received.")
            return {}, 0.0, 0.0, 0.0

        query_clean = query.strip().lower()
        query_emb = self.model.encode(query_clean, normalize_embeddings=True)
        
        if combinedtype == 'Y':
            emb_scores = cosine_similarity([query_emb], self.embedding_company_stocks)[0]
        else:
            emb_scores = cosine_similarity([query_emb], self.embedding_stocks)[0]

        fuzzy_scores = np.array([
            max(
                token_sort_ratio(query_clean, entry["Company_Name"].lower()),
                token_sort_ratio(query_clean, entry["NSE"].lower())
            ) / 100.0
            for entry in self.symbol_entries
        ])
        self.logger.info("Computed fuzzy string scores.")

        combined = self.embedding_weight * emb_scores + self.fuzzy_weight * fuzzy_scores
        best_idx = np.argmax(combined)
        best_score = combined[best_idx]
        best_emb_score = emb_scores[best_idx]
        best_fuzzy_score = fuzzy_scores[best_idx]

        if best_score > threshold:
            self.logger.info(
                "Best match: '%s' (total: %.4f, emb: %.4f, fuzzy: %.4f)",
                self.symbol_entries[best_idx].get("NSE"),
                best_score, best_emb_score, best_fuzzy_score
            )
            return self.symbol_entries[best_idx], best_emb_score, best_fuzzy_score, best_score
        
        self.logger.warning(
            "No match for query '%s' above threshold %.2f. Best: '%s' (total: %.4f, emb: %.4f, fuzzy: %.4f)",
            query_clean, threshold, self.symbol_entries[best_idx].get("NSE"),
            best_score, best_emb_score, best_fuzzy_score
        )
        return {}, best_emb_score, best_fuzzy_score, best_score

    
    async def update_input_data_livesquack(self, data: List[Dict]) -> List[Dict]:
        await self._load_embeddings_async()
        stock_groups = defaultdict(list)
        matched_docs = []
        No_match_stocks = 0
        No_match_entries = 0
        self.logger.info("Processing input data, total docs: %d", len(data))
        for doc in data:
            key = doc.get("nse_symbol")
            news_id = doc.get("news_id")
            company_name = doc.get("custom_name")
            query = f"{company_name} {key}" if company_name else key
            is_combined_query = 'Y' if company_name else "N"
            stock_groups[key].append({
                "news_id":news_id,
                "company_name": company_name,
                'combined': is_combined_query,
                "query": query,
                "original_doc": doc
            })
            
        for stock, entries in stock_groups.items():
            main_entry = next((e for e in entries if e["company_name"]), entries[0])
            match, emb_score, fuzzy_score, total_score = self.get_best_match(
                main_entry["query"], main_entry["combined"], stock=stock
            )
            if match:
                symbolmap = {"NSE": match['NSE'], "BSE":match['BSE'], "Company_Name":match['Company_Name'], "SELECTED":match['SELECTED']}
                for entry in entries:
                    entry["original_doc"]["company"] = match['company']
                    entry["original_doc"]["symbolmap"] = symbolmap
                    matched_docs.append(entry["original_doc"])
                self.logger.info("Matched stock '%s' to symbolmap: %s (total: %.4f, emb: %.4f, fuzzy: %.4f)", stock, str(match), total_score, emb_score, fuzzy_score)
            else:
                already_news_id = []
                for entry in entries:
                    if entry['news_id'] not in already_news_id:
                        entry["original_doc"]["nse_symbol"] = ""
                        matched_docs.append(entry["original_doc"])
                        self.logger.info(f"this news_id :{entry['news_id']} upbated by symbolmap updator , Not Match found so nse_symbol key update to empty")
                        already_news_id.append(entry['news_id'])
                No_match_stocks += 1
                No_match_entries += len(entries)
                self.logger.warning("No match found for stock '%s' (query: '%s'). Best: (total: %.4f, emb: %.4f, fuzzy: %.4f)", stock, main_entry["query"], total_score, emb_score, fuzzy_score)
        self.logger.info("Finished processing input data. Matched docs: %d", len(matched_docs))
        self.logger.info(f"Processed Total stocks unmatched stocks :{No_match_stocks} with total doc count is {No_match_entries} , Note: stock may have multiple docs")
        return matched_docs
