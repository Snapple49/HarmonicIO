
class BinPacking():

    @staticmethod
    def first_fit(input_list, bin_layout, size_descriptor):
        """
        perform a first fit bin packing on the input list, using the alredy existing list of available bins if provided
        """
        bins = []
        if bin_layout:
            bins = bin_layout

        # for each item in the list, go through list from left to right and check if it fits in bin and pack it 
        for item in input_list:
            item_packed = False
            for bin_ in bins:
                if bin_.pack(item, size_descriptor):
                    item_packed = True
                    break
                    
            # otherwise make new bin
            if not item_packed:
                bins.append(Bin(len(bins)))
                if bins[len(bins)-1].pack(item, size_descriptor):
                    item_packed = True

        return bins


class Bin():
    
    class ContainerBinStatus():
        PACKED = "packed"
        QUEUED = "queued"
        RUNNING = "running"
    
    class Item():
        def __init__(self, data, size_descriptor):
            self.size_descriptor = size_descriptor
            self.size = data[self.size_descriptor]
            self.data = data

        def __str__(self):
            return "Item size: {} | data: {}".format(self.size, self.data)

    def __init__(self, bin_index):
        self.items = []
        self.free_space = 1.0
        self.index = bin_index
        self.space_margin = 0.05

    def pack(self, item_data, size_descriptor):
        item = self.Item(item_data, size_descriptor)
        if item.size < self.free_space - self.space_margin:
            self.items.append(item)
            self.free_space -= item.size
            item.data["bin_index"] = self.index
            item.data["bin_status"] = self.ContainerBinStatus.PACKED
            return True
        else:
            del item  
            return False

    def remove_item_in_bin(self, identifier, target):
        for i in range(len(self.items)):
            if self.items[i].data[identifier] == target[identifier]:
                self.free_space += self.items[i].size
                if self.free_space > 1.0:
                    self.free_space = 1.0
                del self.items[i]
                break

    def update_items_in_bin(self, identifier, update_data):
        for item in self.items:
            if item.data[identifier] == update_data[identifier]:
                self.free_space += item.size
                for field in update_data:
                    item.data[field] = update_data[field]
                item.size = item.data[item.size_descriptor]
                self.free_space -= item.size

    def __str__(self):
        bin_items = []
        for item in self.items:
            bin_items.append(str(item))
        return ("Bin index: {}, space: {}, items:\n{}\n".format(self.index, self.free_space, bin_items))

    