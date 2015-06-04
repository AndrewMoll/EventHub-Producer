using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataProducer
{
    class DataObject
    {
        public string FirstName { get; set; }
        public int GeographyId { get; set; }
        public int ItemId { get; set; }
        public double ItemPrice { get; set; }
        public string CreditCardNumber { get; set; }

        public int PageId = 801;
        public string TransactionDateTime = DateTime.Now.ToString();

    }
}
