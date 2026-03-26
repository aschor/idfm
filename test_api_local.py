import asyncio
import json
import logging
import os
import hashlib
from urllib.parse import urlparse

import aiohttp
from idfm_api import IDFMApi
from idfm_api.models import TransportType

# ==============================================================================
# CONFIGURATION DU TEST
# ==============================================================================
# Remplacez ces valeurs par celles que vous avez saisies dans Home Assistant.
# ==============================================================================

# Essayer de charger les variables depuis .env si présent
if os.path.exists(".env"):
    with open(".env", "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                os.environ[k] = v.strip('"').strip("'")

TOKEN = os.environ.get("IDFM_TOKEN", "VOTRE_TOKEN_API") 
TRANSPORT = "TRAIN" # Options habituelles: "METRO", "TRAM", "BUS", "TRAIN"
LINE_ID = "C01744" # Ligne N
STOP_NAME_CITY = "Rambouillet - Rambouillet"

TRANSPORT = "BUS" # Options habituelles: "METRO", "TRAM", "BUS", "TRAIN"
LINE_ID = "C00183" #bus 5302 # la ligne est indisponible au 26/03/2026
# LINE_ID = "C02638" #bus 5304
STOP_NAME_CITY = "Maréchal Juin - Rambouillet"
#2026-03-26 11:00:35,349 - INFO - stop : StopData(name='Rambouillet', stop_id='STIF:StopPoint:Q:427870:', x='48.644638872398815', y='1.8337103324105775', zip_code='78517', city='Rambouillet', exchange_area_id='STIF:StopArea:SP:60665:', exchange_area_name='Rambouillet')
# ==============================================================================

# --- LOGGING ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("idfm_debug.log", mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CACHE_DIR = "idfm_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

class MockResponse:
    def __init__(self, data, status=200):
        self._data = data
        self.status = status

    async def json(self, **kwargs):
        if isinstance(self._data, (dict, list)):
            return self._data
        return json.loads(self._data)
        
    async def text(self, **kwargs):
        if isinstance(self._data, (dict, list)):
            return json.dumps(self._data)
        return self._data

    async def read(self):
        return (await self.text()).encode('utf-8')

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


class CachingClientSession:
    """Proxy interceptant les requêtes pour les mettre en cache local."""
    def __init__(self, real_session: aiohttp.ClientSession):
        self.real_session = real_session

    def get_cache_filename(self, url, params):
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_")
        
        param_str = ""
        if params:
            param_str = "_".join(f"{k}-{v}" for k, v in sorted(params.items()))
            
        base_name = f"{path}_{param_str}" if param_str else path
        # Hashing pour éviter des noms de fichiers trop longs/invalides
        hash_suffix = hashlib.md5((url + str(params)).encode()).hexdigest()[:8]
        
        # Nom convivial + hash
        safe_prefix = "".join(c for c in base_name if c.isalnum() or c == '_')[:70]
        
        return os.path.join(CACHE_DIR, f"{safe_prefix}_{hash_suffix}.json")

    def get(self, url, params=None, **kwargs):
        return self._request_context("GET", url, params, **kwargs)

    def getattr(self, name):
        # Transférer au real_session si non trouvé
        return getattr(self.real_session, name)

    def _request_context(self, method, url, params=None, **kwargs):
        class RequestContext:
            def __init__(self, parent_session, url, params, kwargs):
                self.parent_session = parent_session
                self.url = url
                self.params = params
                self.kwargs = kwargs

            async def __aenter__(self):
                cache_file = self.parent_session.get_cache_filename(self.url, self.params)
                
                if os.path.exists(cache_file):
                    logger.info(f"CACHE HIT: Chargement depuis {cache_file} (évite un appel API)")
                    with open(cache_file, "r", encoding="utf-8") as f:
                        data = f.read()
                    return MockResponse(data)
                
                logger.info(f"CACHE MISS: Appel réseau {self.url}")
                # Appel réel
                self.real_resp = await self.parent_session.real_session.get(
                    self.url, params=self.params, **self.kwargs
                )
                
                # Récupère et sauvegarde
                try:
                    text_data = await self.real_resp.text()
                except Exception as e:
                    logger.error(f"Échec de lecture de la réponse: {e}")
                    raise
                
                if self.real_resp.status == 200:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        f.write(text_data)
                    logger.debug(f"JSON sauvegardé dans {cache_file}")
                else:
                    logger.warning(f"Statut HTTP {self.real_resp.status}, sauvegarde ignorée.")
                    # On le sauvegarde quand même avec un tag erreur (utile pour le debug)
                    err_cache_file = cache_file.replace(".json", ".err.json")
                    with open(err_cache_file, "w", encoding="utf-8") as f:
                        f.write(text_data)
                
                return MockResponse(text_data, status=self.real_resp.status)

            async def __aexit__(self, exc_type, exc, tb):
                if hasattr(self, 'real_resp'):
                    self.real_resp.close()

            def __await__(self):
                async def _get_response():
                    return await self.__aenter__()
                return _get_response().__await__()

        return RequestContext(self, url, params, kwargs)


async def main():
    if TOKEN == "VOTRE_TOKEN_API":
        logger.error("Veuillez éditer le script pour renseigner la variable TOKEN.")
        return

    logger.info("Démarrage du script de test IDFM local")
    
    transport_enum = None
    for t in list(TransportType):
        if t.name == TRANSPORT:
            transport_enum = t
            break
            
    if not transport_enum:
        logger.error(f"Type '{TRANSPORT}' non reconnu. Valeurs valides: {[t.name for t in list(TransportType)]}")
        return

    async with aiohttp.ClientSession() as real_session:
        caching_session = CachingClientSession(real_session)
        
        # Init
        client = IDFMApi(caching_session, TOKEN, timeout=300)
        
        try:
            config_data = {}
            
            # 1. Lignes (Correspond à async_step_line dans config_flow)
            logger.info(f"=== Etape 1 : Récupération des lignes ({TRANSPORT}) === ( {transport_enum=} )")
            lines = await client.get_lines(transport_enum) #rail
            
            for l in lines:
                logger.info(f"Ligne : {l.name} (id: {l.id})")
                if l.id == LINE_ID:
                    config_data["line_id"] = l.id
                    config_data["line_name"] = l.name
                    logger.info(f"Ligne retenue : {l.name} (id: {l.id})")
                    break
                    
            if "line_id" not in config_data:
                logger.error(f"Ligne '{LINE_ID}' introuvable. Essayer parmi les IDs affichés précédemment.")
                return
                
            # 2. Gares (Correspond à async_step_stop dans config_flow)
            logger.info(f"=== Etape 2 : Récupération des gares pour la ligne {config_data['line_id']} ===")
            stops = await client.get_stops(config_data["line_id"])
            
            for s in stops:
                full_name = f"{s.name} - {s.city}"
                # logger.info(f"stop : {s}")
                if full_name == STOP_NAME_CITY:
                    # STRICTEMENT COMME LE CONFIG_FLOW CORRIGE :
                    config_data["stop_id"] = s.exchange_area_id or s.stop_id
                    config_data["stop_name"] = full_name
                    logger.info(f"Gare retenue : {full_name} (exchange_area_id: {s.exchange_area_id}, stop_id: {s.stop_id} => id final: {config_data['stop_id']})")
                    break
                    
            if "stop_id" not in config_data:
                logger.error(f"Gare '{STOP_NAME_CITY}' introuvable. Quelques gares valides : {[s.name + ' - ' + s.city for s in stops[:1000]]}...")
                return

            # 3. Directions & Destinations (Correspond à async_step_direction)
            logger.info("=== Etape 3 : Récupération des directions et destinations ===")
            try:
                directions = await client.get_directions(config_data["stop_id"], line_id=config_data["line_id"])
                logger.info(f"Directions : {directions}")
                
            except json.JSONDecodeError as jde:
                logger.error(f"PLANTAGE (Directions) - JSONDecodeError: {jde}")
            except Exception as e:
                logger.error(f"PLANTAGE (Directions) - Autre Erreur: {e}")
                
            try:
                destinations = await client.get_destinations(config_data["stop_id"], line_id=config_data["line_id"])
                logger.info(f"Destinations : {destinations}")
                
            except json.JSONDecodeError as jde:
                logger.error(f"PLANTAGE (Destinations) - JSONDecodeError: {jde}")
            except Exception as e:
                logger.error(f"PLANTAGE (Destinations) - Autre Erreur: {e}")
                
            logger.info("Test terminé.")
            
        except Exception as global_err:
            logger.exception(f"Une erreur fatale non interceptée s'est produite: {global_err}")

if __name__ == "__main__":
    asyncio.run(main())
