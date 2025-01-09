#---------------------------------------------------------------------------
#
# RUN ALL STEPS:
#
# - simple bash script for running all the steps
# - to be run from root directory of the repo (. scripts/run.sh)
# - this will be replaced with a proper task manager in future versions
#
#---------------------------------------------------------------------------


echo '******************************************************'
date;
echo '******************************************************'


echo
echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-0.samples_and_qdann_yield.py";
echo '-----------------------------------------------------'
date;
python scripts/step-0.samples_and_qdann_yield.py


echo
echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-1.export_cdl_corn_soy.py";
echo '-----------------------------------------------------'
date;
python scripts/step-1.export_cdl_corn_soy.py


echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-2.export_landsat_data.py";
echo '-----------------------------------------------------'
date;
python scripts/step-2.export_landsat_data.py


echo
echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-3.raw_spectral_indices.py";
echo '-----------------------------------------------------'
date;
python scripts/step-3.raw_spectral_indices.py

echo
echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-4.gap_filling_and_smoothing.py";
echo '-----------------------------------------------------'
date;
python scripts/step-4.gap_filling_and_smoothing.py

echo
echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-5.indices-stats.py";
echo '-----------------------------------------------------'
date;
python scripts/step-5.indices-stats.py


echo
echo
echo
echo
echo
echo '-----------------------------------------------------'
echo "step-6.cover_crop_features.py";
echo '-----------------------------------------------------'
date;
python scripts/step-6.cover_crop_features.py


echo
echo
echo
echo
echo
echo '******************************************************'
date;
echo '******************************************************'
