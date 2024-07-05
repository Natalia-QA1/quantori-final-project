import logging
from abc import ABC, abstractmethod

from rdkit import DataStructs, Chem
from rdkit.Chem import AllChem


class RdkitComputationsBaseException(
    Exception
):
    pass


class RdkitComputationsComputationException(
    RdkitComputationsBaseException
):
    pass


class RdkitComputationsCommonException(
    RdkitComputationsBaseException
):
    pass


class MorganFingerprintGenerator:

    def __init__(self):
        pass

    @staticmethod
    def compute_fingerprint(smiles, radius=2, nbits=2048):
        """
        Computes the Morgan fingerprint for a given SMILES string.

        :param smiles: The SMILES representation of the molecule.
        :param radius: The radius of the circular fingerprint.
         Default is 2.
        :param nbits: The number of bits in the fingerprint.
         Default is 2048.
        :return: The binary string representation of the fingerprint,
         or None if invalid SMILES.
        """
        try:
            mol = Chem.MolFromSmiles(smiles)
            if mol is not None:
                fp = AllChem.GetMorganFingerprintAsBitVect(
                    mol,
                    radius,
                    nBits=nbits
                )
                return fp.ToBitString()
            else:
                logging.error(
                    f"Invalid SMILES string: {smiles}"
                )
                return None

        except (ValueError, RuntimeError) as rdkit_error:
            raise RdkitComputationsComputationException(
                f"RDKit error computing fingerprint for SMILES"
                f"{smiles}': {str(rdkit_error)}"
            )
        except (AttributeError, TypeError) as g_error:
            raise RdkitComputationsCommonException(
                f"Generic error computing fingerprint for SMILES"
                f"{smiles}: {str(g_error)}"
            )


class TanimotoSimilarityGenerator(ABC):

    def __init__(self):
        pass

    def compute_tanimoto(self, fp1, fp2):
        """
        Computes the Tanimoto similarity between two fingerprints.
        :param fp1: The first fingerprint.
        :param fp2: The second fingerprint.
        :return: The Tanimoto similarity score.
        """
        return DataStructs.FingerprintSimilarity(fp1, fp2)

    @abstractmethod
    def compute_similarity_scores(self, df_1, df_2):
        """
        Abstract method to compute similarity scores.

        This method should be implemented to perform the similarity
        computation for specific data tables, where particular names
        or structures are used. The exact implementation will depend
        on the specific use case and data source.

        :return: The computed similarity scores.
        """
        pass


class TanimotoSimilarityForMoleculesFromAWS(
    TanimotoSimilarityGenerator
):
    def compute_similarity_scores(self, df_1, df_2):
        similarity_scores = []

        for _, target_row in df_1.iterrows():
            target_fp = DataStructs.CreateFromBitString(
                target_row["fingerprint"]
            )
            chembl_ids = []
            similarities = []

            for _, chembl_row in df_2.iterrows():
                chembl_fp = DataStructs.CreateFromBitString(
                    chembl_row["fingerprint"]
                )
                similarity = self.compute_tanimoto(
                    target_fp,
                    chembl_fp
                )
                chembl_ids.append(
                    chembl_row["chembl_id"]
                )
                similarities.append(
                    similarity
                )
            similarity_scores.append({
                "target_chembl_id": target_row["molecule_name"],
                "source_chembl_id": chembl_ids,
                "similarity_score": similarities
            })

        return similarity_scores
