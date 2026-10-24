# Data folder

The MovieLens CSV files are **not committed** to this repository. Download them as follows.

## Download

1. Go to <https://grouplens.org/datasets/movielens/latest/> and download
   **ml-latest-small.zip** ("Small: 100,000 ratings ... 9,000 movies ... 600 users").
   Direct link: <https://files.grouplens.org/datasets/movielens/ml-latest-small.zip>
2. Unzip it and copy these three files into this `data/` folder:
   - `movies.csv`
   - `ratings.csv`
   - `links.csv` (IMDb IDs, used for exact OMDb lookups)

Windows PowerShell, from the project root:

```powershell
$ProgressPreference = 'SilentlyContinue'
Invoke-WebRequest -Uri "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip" -OutFile "data\ml-latest-small.zip"
Expand-Archive -Path "data\ml-latest-small.zip" -DestinationPath "data" -Force
Move-Item data\ml-latest-small\movies.csv, data\ml-latest-small\ratings.csv, data\ml-latest-small\links.csv -Destination data -Force
Remove-Item data\ml-latest-small, data\ml-latest-small.zip -Recurse -Force
```

## Expected files

| File | Data rows | SHA-256 |
|---|---|---|
| `movies.csv` | 9,742 | `5A5F32DD9BB3797B8E728A1B98958789D2B13F294A69FDFBC5727F8A9611AA07` |
| `ratings.csv` | 100,836 | `AA289CA83157595D0DF6AEA1BE6A4DED676DDC4385472E8313A8ED9805352646` |
| `links.csv` | 9,742 | `97AD18E4E56A09363C65676B6CB3482CE3E2CEA2372A24620C1599C843325F31` |

Check with `Get-FileHash data\*.csv -Algorithm SHA256` (PowerShell) or `sha256sum data/*.csv`.

## Licence

The MovieLens data is provided by GroupLens Research (University of Minnesota). Its licence
allows redistribution under the same terms but not commercial use without permission. The files
are kept out of this repository so the repository holds code only and reviewers use the
official, unmodified files. See the README inside the downloaded zip for the full terms.