import marimo

__generated_with = "0.11.2"
app = marimo.App(width="medium", app_title="Marimo Viewer: Cirro")


@app.cell
def _(mo):
    mo.md(r"""# Marimo Viewer: Cirro""")
    return


@app.cell
def _():
    # Define the types of datasets which can be read in
    # This is used to filter the dataset selector, below
    cirro_dataset_type_filter = [
        "custom_files"
    ]
    return (cirro_dataset_type_filter,)


@app.cell
def _():
    # Load the marimo library in a dedicated cell for efficiency
    import marimo as mo
    return (mo,)


@app.cell
def _():
    # If the script is running in WASM (instead of local development mode), load micropip
    import sys
    if "pyodide" in sys.modules:
        import micropip
        running_in_wasm = True
    else:
        micropip = None
        running_in_wasm = False
    return micropip, running_in_wasm, sys


@app.cell
async def _(micropip, mo, running_in_wasm):
    with mo.status.spinner("Loading dependencies"):
        # If we are running in WASM, some dependencies need to be set up appropriately.
        # This is really just aligning the needs of the app with the default library versions
        # that come when a marimo app loads in WASM.
        if running_in_wasm:
            print("Installing via micropip")
            # Downgrade plotly to avoid the use of narwhals
            await micropip.install("plotly<6.0.0")
            await micropip.install("ssl")
            micropip.uninstall("urllib3")
            micropip.uninstall("httpx")
            await micropip.install(["urllib3==2.3.0"])
            await micropip.install([
                "boto3==1.36.3",
                "botocore==1.36.3"
            ], verbose=True)
            await micropip.install(["cirro[pyodide]>=1.2.16"], verbose=True)

        from io import StringIO, BytesIO
        from queue import Queue
        from time import sleep
        from typing import Dict, Optional
        import plotly.express as px
        import pandas as pd
        import numpy as np
        from functools import lru_cache
        import base64
        from urllib.parse import quote_plus
        from anndata import AnnData
        import sklearn

        from cirro import DataPortalLogin, DataPortalDataset
        from cirro.services.file import FileService
        from cirro.sdk.file import DataPortalFile
        from cirro.config import list_tenants

        # A patch to the Cirro client library is applied when running in WASM
        if running_in_wasm:
            from cirro.helpers import pyodide_patch_all
            pyodide_patch_all()
    return (
        AnnData,
        BytesIO,
        DataPortalDataset,
        DataPortalFile,
        DataPortalLogin,
        Dict,
        FileService,
        Optional,
        Queue,
        StringIO,
        base64,
        list_tenants,
        lru_cache,
        np,
        pd,
        px,
        pyodide_patch_all,
        quote_plus,
        sklearn,
        sleep,
    )


@app.cell
def _(mo):
    # Get and set the query parameters
    query_params = mo.query_params()
    return (query_params,)


@app.cell
def _(list_tenants):
    # Get the tenants (organizations) available in Cirro
    tenants_by_name = {i["displayName"]: i for i in list_tenants()}
    tenants_by_domain = {i["domain"]: i for i in list_tenants()}


    def domain_to_name(domain):
        return tenants_by_domain.get(domain, {}).get("displayName")


    def name_to_domain(name):
        return tenants_by_name.get(name, {}).get("domain")
    return (
        domain_to_name,
        name_to_domain,
        tenants_by_domain,
        tenants_by_name,
    )


@app.cell
def _(mo):
    mo.md(r"""## Load Data""")
    return


@app.cell
def _(mo):
    # Use a state element to manage the Cirro client object
    get_client, set_client = mo.state(None)
    return get_client, set_client


@app.cell
def _(domain_to_name, mo, query_params, tenants_by_name):
    # Let the user select which tenant to log in to (using displayName)
    domain_ui = mo.ui.dropdown(
        options=tenants_by_name,
        value=domain_to_name(query_params.get("domain")),
        on_change=lambda i: query_params.set("domain", i["domain"]),
        label="Load Data from Cirro",
    )
    domain_ui
    return (domain_ui,)


@app.cell
def _(DataPortalLogin, domain_ui, get_client, mo):
    # If the user is not yet logged in, and a domain is selected, then give the user instructions for logging in
    # The configuration of this cell and the two below it serve the function of:
    #   1. Showing the user the login instructions if they have selected a Cirro domain
    #   2. Removing the login instructions as soon as they have completed the login flow
    if get_client() is None and domain_ui.value is not None:
        with mo.status.spinner("Authenticating"):
            # Use device code authorization to log in to Cirro
            cirro_login = DataPortalLogin(base_url=domain_ui.value["domain"])
            cirro_login_ui = mo.md(cirro_login.auth_message_markdown)
    else:
        cirro_login = None
        cirro_login_ui = None

    mo.stop(cirro_login is None)
    cirro_login_ui
    return cirro_login, cirro_login_ui


@app.cell
def _(cirro_login, set_client):
    # Once the user logs in, set the state for the client object
    set_client(cirro_login.await_completion())
    return


@app.cell
def _(get_client, mo):
    # Get the Cirro client object (but only take action if the user selected Cirro as the input)
    client = get_client()
    mo.stop(client is None)
    return (client,)


@app.cell
def _():
    # Helper functions for dealing with lists of objects that may be accessed by id or name
    def id_to_name(obj_list: list, id: str) -> str:
        if obj_list is not None:
            return {i.id: i.name for i in obj_list}.get(id)


    def name_to_id(obj_list: list) -> dict:
        if obj_list is not None:
            return {i.name: i.id for i in obj_list}
        else:
            return {}
    return id_to_name, name_to_id


@app.cell
def _(client):
    # Set the list of projects available to the user
    projects = client.list_projects()
    projects.sort(key=lambda i: i.name)
    return (projects,)


@app.cell
def _(id_to_name, mo, name_to_id, projects, query_params):
    # Let the user select which project to get data from
    project_ui = mo.ui.dropdown(
        value=id_to_name(projects, query_params.get("project")),
        options=name_to_id(projects),
        on_change=lambda i: query_params.set("project", i)
    )
    project_ui
    return (project_ui,)


@app.cell
def _(client, mo, project_ui):
    # Stop if the user has not selected a project
    mo.stop(project_ui.value is None)

    # Get the list of datasets available to the user
    # Filter the list of datasets by type (process_id)
    datasets = [
        dataset
        for dataset in client.get_project_by_id(project_ui.value).list_datasets()
        # if dataset.process_id in cirro_dataset_type_filter
    ]
    return (datasets,)


@app.cell
def _(datasets, id_to_name, mo, name_to_id, query_params):
    # Let the user select which dataset to get data from
    dataset_ui = mo.ui.dropdown(
        value=id_to_name(datasets, query_params.get("dataset")),
        options=name_to_id(datasets),
        on_change=lambda i: query_params.set("dataset", i)
    )
    dataset_ui
    return (dataset_ui,)


@app.cell
def _(
    AnnData,
    DataPortalDataset,
    DataPortalFile,
    Dict,
    client,
    dataset_ui,
    pd,
    project_ui,
):
    # Read the selected dataset from Cirro as an AnnData object
    def read_de_andata(file: DataPortalFile, sample_meta: pd.DataFrame) -> AnnData:
        # Read in as a TSV
        df = file.read_csv(sep="\t")

        # Use the GeneID as the unique index
        df.set_index("GeneID", inplace=True)

        # Find the columns which are sample readcounts
        samples = [
            cname for cname in df.columns
            if cname in sample_meta.index
        ]
        assert len(samples) > 0, (df.columns, sample_meta.index)

        return AnnData(
            X=df.reindex(columns=samples).astype(int).values,
            obs=df.drop(columns=samples),
            var=sample_meta.reindex(index=samples)
        )

    class DE:
        # Make a dedicated object that contains the complete readcounts, the DE results, and the sample metadata

        ds: DataPortalDataset

        # Sample metadata
        meta: pd.DataFrame

        # DE results: dict of AnnData objects
        results: Dict[str, pd.DataFrame]

        # Single table with counts for everything
        counts: pd.DataFrame

        # Counts-per-million
        cpm: pd.DataFrame

        # Find all of the different groups in the sample metadata
        groups: list

        def __init__(self, ds: DataPortalDataset):

            self.ds = ds
            self._files = ds.list_files()
            self.read_meta()
            self.read_de()
            self.merge_counts()
            self.calc_cpm()
            self.infer_groups()

        def read_meta(self):
            # Read in the metadata file
            self.meta = self._files.get_by_id("data/meta.tsv").read_csv(sep="\t").set_index("Sample")

        def read_de(self):
            # Load in all of the files with the name DE_all.tsv
            # Each file will be loaded in as its own AnnData object,
            # and we will give it an ID using the name of the folder it is inside of
            self.results = {}
            for file in self._files:
                if file.name.split("/")[-1] == "DE_all.tsv":
                    self.results[file.absolute_path.split("/")[-2]] = read_de_andata(file, self.meta)

        def merge_counts(self):
            # Make a single DataFrame with all samples and all genes
            self.counts = pd.DataFrame(
                index=list(set([
                    i
                    for adata in self.results.values()
                    for i in adata.var_names
                ])),
                columns=list(set([
                    i
                    for adata in self.results.values()
                    for i in adata.obs_names
                ]))
            )
            # Populate the counts from each DE results
            for adata in self.results.values():
                # Iterate over each sample's data
                for sample_name, readcounts in adata.to_df().items():
                    self.counts.loc[sample_name, readcounts.index] = readcounts.values

        def calc_cpm(self):
            self.cpm = self.counts.apply(
                lambda r: r / (r.sum() / 1e6),
                axis=1
            )

        def infer_groups(self):
            self.groups = list(set([
                val
                for _, cvals in self.meta.items()
                for val in cvals.values
            ]))

    de = DE(
        client
        .get_project_by_id(project_ui.value)
        .get_dataset_by_id(dataset_ui.value)
    )
    return DE, de, read_de_andata


@app.cell(hide_code=True)
def _(de, mo):
    # Get the inputs for plotting PCA
    pca_params = (
        mo.md(
    """
    ### Options for Plotting PCA

    - Sample Groups to Include: {sample_groups}
    - Color By: {color_by}
    """)
        .batch(
            sample_groups=mo.ui.multiselect(
                options=de.groups,
                value=de.groups
            ),
            color_by=mo.ui.dropdown(
                options=de.meta.columns,
                value=de.meta.columns.values[0]
            )
        )
    )

    pca_params
    return (pca_params,)


@app.cell
def _(de, pca_params, pd, px, sklearn):
    def plot_pca():
        # Filter to the samples of interest
        samples_to_include = list(set([
            sample
            for _, cvals in de.meta.items()
            for sample, val in cvals.items()
            if val in pca_params.value["sample_groups"]
        ]))
        assert len(samples_to_include) > 2, "Need at least 3 samples to plot"

        # Get the gene counts for those samples
        cpm = de.cpm.reindex(index=samples_to_include).fillna(0)

        # Get the PCA coordinates
        pca = sklearn.decomposition.PCA()
        _coords = pca.fit_transform(cpm.values)
        pca_coords = pd.DataFrame(
            _coords,
            index=cpm.index,
            columns=[
                f"PC{i+1} ({round(var_explained * 100, 1)}%)"
                for i, var_explained in enumerate(pca.explained_variance_ratio_)
            ]
        ).assign(**{
            pca_params.value["color_by"]: de.meta[pca_params.value["color_by"]]
        })

        fig = px.scatter(
            pca_coords,
            x=pca_coords.columns.values[0],
            y=pca_coords.columns.values[1],
            template="simple_white",
            hover_name=pca_coords.index,
            color=pca_params.value["color_by"]
        )
        return fig

    plot_pca()
    return (plot_pca,)


@app.cell
def _(mo):
    mo.md(r"""### Differential Expression Results""")
    return


@app.cell
def _(de, mo):
    # User inputs which comparison to inspect
    select_comparison_ui = mo.ui.dropdown(
        label="Select Comparison:",
        options=de.results
    )
    select_comparison_ui
    return (select_comparison_ui,)


@app.cell
def _(mo):
    select_fdr_cutoff_ui = mo.ui.number(
        label="Cutoff - FDR:",
        value=0.05,
        start=0.,
        stop=1.,
        step=0.01
    )
    select_fdr_cutoff_ui
    return (select_fdr_cutoff_ui,)


@app.cell
def _(mo):
    select_lfc_cutoff_ui = mo.ui.number(
        label="Cutoff - Absolute Log Fold Change:",
        value=1,
        start=0.,
        step=0.1
    )
    select_lfc_cutoff_ui
    return (select_lfc_cutoff_ui,)


@app.cell
def _(
    np,
    px,
    select_comparison_ui,
    select_fdr_cutoff_ui,
    select_lfc_cutoff_ui,
):
    def plot_volcano():
        adata = select_comparison_ui.value
        if adata is None:
            return

        fdr_cutoff = select_fdr_cutoff_ui.value
        lfc_cutoff = select_lfc_cutoff_ui.value

        plot_df = (
            adata.obs
            .assign(
                neg_log10_pvalue=adata.obs['PValue'].apply(np.log10) * -1,
                is_sig=(
                    (adata.obs['FDR'] <= fdr_cutoff)
                    &
                    (adata.obs["logFC"].abs() >= lfc_cutoff)
                )
            )
        )

        fig = px.scatter(
            plot_df,
            x="logFC",
            y="neg_log10_pvalue",
            template="simple_white",
            color="is_sig",
            hover_data=["FDR"],
            hover_name="GeneName",
            labels=dict(
                neg_log10_pvalue="p-value (-log10)",
                logFC="Fold Change (log2)",
                is_sig=f"FDR <= {fdr_cutoff}<br>LFC >= {lfc_cutoff}"
            )
        )

        return fig

    plot_volcano()
    return (plot_volcano,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
