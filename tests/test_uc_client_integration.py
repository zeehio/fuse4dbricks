import os
import pytest

from fuse4dbricks.api.uc_client import UnityCatalogClient

# ==========================================
# CATEGORÍA 2: Tests LIVE (Contra Databricks Real)
# ==========================================

# Solo ejecutar si existen variables de entorno
requires_databricks = pytest.mark.skipif(
    not (os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN")),
    reason="Requiere DATABRICKS_HOST y DATABRICKS_TOKEN",
)


@pytest.fixture
def live_client():
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    # Mock simple de Auth Provider que devuelve el token de entorno
    class EnvAuth:
        def get_access_token(self, force_refresh=False):
            return token

    client = UnityCatalogClient(host, EnvAuth())
    return client


@requires_databricks
@pytest.mark.trio
async def test_live_hierarchy_traversal(live_client):
    """
    Smoke Test: Intenta listar catálogos reales.
    Esto valida que la URL base y el endpoint de la API sean correctos.
    """
    try:
        catalogs = await live_client.list_catalogs()
        print(f"\nFound {len(catalogs)} catalogs.")

        # Validar estructura básica
        if len(catalogs) > 0:
            assert "name" in catalogs[0]

            # Profundizar un nivel (Opcional, si tienes permisos)
            cat_name = catalogs[0]["name"]
            schemas = await live_client.list_schemas(cat_name)
            print(f"Found {len(schemas)} schemas in {cat_name}.")

    finally:
        await live_client.close()


@requires_databricks
@pytest.mark.trio
async def test_live_404_handling(live_client):
    """Valida que un path inexistente devuelva None o [] según corresponda."""
    try:
        # Metadatos de archivo fantasma -> None
        meta = await live_client.get_file_metadata(
            "/Volumes/main/default/non_existent/ghost.txt"
        )
        assert meta is None

        # Lista de carpeta fantasma -> []
        contents = await live_client.list_directory_contents(
            "/Volumes/main/default/non_existent_folder"
        )
        assert contents == []
    finally:
        await live_client.close()
