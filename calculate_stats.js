const fs = require('fs');
const path = require('path');

/**
 * Calculate total number of brands and phones from MobiInfo/Brands directory
 */
function calculateMobiInfoStats() {
    const brandsDir = path.join(__dirname, 'MobiInfo', 'Brands');
    
    try {
        // Check if Brands directory exists
        if (!fs.existsSync(brandsDir)) {
            console.error('❌ MobiInfo/Brands directory not found!');
            return;
        }
        
        // Read all files in the Brands directory
        const files = fs.readdirSync(brandsDir);
        const jsonFiles = files.filter(file => file.endsWith('.json'));
        
        if (jsonFiles.length === 0) {
            console.log('📂 No brand files found in MobiInfo/Brands directory');
            return;
        }
        
        let totalBrands = 0;
        let totalPhones = 0;
        const brandDetails = [];
        
        // Process each brand file
        jsonFiles.forEach(file => {
            try {
                const filePath = path.join(brandsDir, file);
                const fileContent = fs.readFileSync(filePath, 'utf8');
                const brandData = JSON.parse(fileContent);
                
                if (brandData.brand_info && brandData.phones) {
                    const brandName = brandData.brand_info.name;
                    const phoneCount = brandData.phones.length;
                    const lastUpdated = brandData.brand_info.last_updated;
                    
                    totalBrands++;
                    totalPhones += phoneCount;
                    
                    brandDetails.push({
                        name: brandName,
                        phones: phoneCount,
                        lastUpdated: lastUpdated,
                        file: file
                    });
                } else {
                    console.log(`⚠️  Invalid format in ${file}`);
                }
            } catch (error) {
                console.log(`❌ Error reading ${file}: ${error.message}`);
            }
        });
        
        // Sort brands by phone count (descending)
        brandDetails.sort((a, b) => b.phones - a.phones);
        
        console.log('\n' + '='.repeat(70));
        console.log('📈 MOBIINFO STATISTICS SUMMARY');
        console.log('='.repeat(70));
        console.log(`📱 Total Brands: ${totalBrands}`);
        console.log(`📞 Total Phones: ${totalPhones}`);
        console.log('='.repeat(70));
        
        // Show all brands by phone count
        console.log('\n🏆 BRANDS BY PHONE COUNT:');
        console.log('-'.repeat(50));
        brandDetails.forEach((brand, index) => {
            const rank = (index + 1).toString().padStart(2);
            const name = brand.name.padEnd(15);
            const phones = brand.phones.toString().padStart(3);
            console.log(`${rank}. ${name} | ${phones} phones | Last Update: ${brand.lastUpdated}`);
        });
        
        // Save detailed report to JSON file
        const report = {
            timestamp: new Date().toISOString(),
            summary: {
                totalBrands: totalBrands,
                totalPhones: totalPhones,
            },
            brandDetails: brandDetails
        };
        
        const reportPath = path.join(__dirname, 'MobiInfo', 'stats_report.json');
        fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
        console.log(`\n💾 Detailed report saved to: ${reportPath}`);
        
    } catch (error) {
        console.error('❌ Error calculating statistics:', error.message);
    }
}

// Run the calculation
if (require.main === module) {
    calculateMobiInfoStats();
}

module.exports = { calculateMobiInfoStats };
